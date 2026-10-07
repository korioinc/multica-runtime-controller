#!/usr/bin/env bash
set -euo pipefail
[[ $# == 1 || ( $# == 2 && $2 == --nfs ) || ( $# == 3 && $2 == --nfs && $3 == --root-loss ) ]] || { echo "Usage: $0 IMAGE [--nfs [--root-loss]]" >&2; exit 2; }
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
endpoint=${DOCKER_HOST:-$(docker context inspect "${DOCKER_CONTEXT:-$(docker context show)}" --format '{{.Endpoints.docker.Host}}')}
[[ $endpoint == unix://* ]] || { echo 'A local Unix Docker endpoint is required.' >&2; exit 1; }
docker_cli=(env -u DOCKER_CONTEXT -u DOCKER_HOST docker --host "$endpoint")
image=$("${docker_cli[@]}" image inspect --format '{{.Id}}' "$1")
platform=$("${docker_cli[@]}" version --format '{{.Server.Os}}/{{.Server.Arch}}')
[[ $platform == linux/arm64 || $platform == linux/amd64 ]] || exit 1
[[ $("${docker_cli[@]}" image inspect --format '{{.Os}}/{{.Architecture}}' "$image") == "$platform" ]] || exit 1
proof=$(mktemp -d "${TMPDIR:-/tmp}/multica-resident-desktop.XXXXXX")
chmod 0755 "$proof"
# shellcheck source=build/runtime-versions.env
source "$root/build/runtime-versions.env"
export GOTOOLCHAIN="go$GO_VERSION"
CGO_ENABLED=0 GOOS=linux GOARCH=${platform#linux/} go -C "$root/src" test -mod=readonly -c -o "$proof/controller.test" ./internal/controller
{
  echo "native_platform=$platform"
  "${docker_cli[@]}" image inspect --format 'image_id={{.Id}} descriptor={{json .Descriptor}}' "$image"
  go version
  shasum -a 256 "$proof/controller.test"
} | tee "$proof/environment.log"
python3 - "$endpoint" "$image" "$proof" "$root" "${2:-}" "${3:-}" <<'PY'
import json, os, pathlib, subprocess, sys, time, uuid

endpoint, image, proof, source_root, transport, fault = sys.argv[1:]
proof = pathlib.Path(proof)
use_nfs = transport == '--nfs'
root_loss = fault == '--root-loss'
docker = ['docker', '--host', endpoint]
env = dict(os.environ)
env.pop('DOCKER_HOST', None)
env.pop('DOCKER_CONTEXT', None)
token = 'resident-' + str(uuid.uuid4())
network, controller, worker = token, token + '-controller', token + '-worker'
private_volume = token + '-private'
layout = token + '-layout'
nfs = token + '-nfs'
workspace_volume = token + '-workspace'
task_volume = token + '-task'
recovery_volume = token + '-recovery'
volumes = [private_volume]
resource_label = ['--label', 'multica.proof=' + token]
session = 'obu-' + token
control, workspace = [proof / name for name in ('control', 'workspace')]
for path in (control, workspace):
    path.mkdir(); path.chmod(0o777)
worker_started = False
browser_instance, browser_finalized = None, False

def run(args, timeout=30):
    result = subprocess.run(docker + args, env=env, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'Docker {args[0]} failed (exit {result.returncode}): ' + (result.stderr + result.stdout)[-3000:])
    return result.stdout + (result.stderr if args[0] == 'logs' else '')

def wait_file(path, timeout=150):
    deadline = time.monotonic() + timeout
    value = read_file(path)
    while value is None:
	# Stop a fixture wait when authoritative container state is terminal.
        state = json.loads(run(['inspect', '--format', '{{json .State}}', controller]))
        if not state['Running']:
            raise RuntimeError('Controller fixture stopped before ' + str(path) + '\n' + run(['logs', controller])[-3000:])
        if worker_started:
            state = json.loads(run(['inspect', '--format', '{{json .State}}', worker]))
            if not state['Running']:
                raise RuntimeError('Worker fixture stopped before ' + str(path) + '\n' + run(['logs', worker])[-3000:])
        if time.monotonic() >= deadline:
            raise RuntimeError('Fixture did not reach ' + str(path))
        time.sleep(.1)
        value = read_file(path)
    return value

def read_file(path):
    if use_nfs and path.is_relative_to(workspace):
        target = '/workspace/' + str(path.relative_to(workspace))
        code = 'import json,pathlib,sys; p=pathlib.Path(sys.argv[1]); print(json.dumps(p.read_text() if p.is_file() else None))'
        return json.loads(run(['exec', controller, '/usr/bin/python3', '-c', code, target]))
    return path.read_text() if path.exists() else None

def cli(binary, args):
    raw = run(['exec', worker, binary] + args, timeout=20)
    value = json.loads(raw)
    if value.get('error') or value.get('isError'):
        raise RuntimeError('Tool refused the proof: ' + raw[:2000])
    return value

def obu(command, *args):
    value = cli('obu', [command, '--session-id', session] + list(args))
    return value.get('result', value)

def cua(command, request):
    request = dict(request, session='resident-acceptance')
    return cli('cua-driver', ['call', command, json.dumps(request)])

def cdp(tab, method, params):
    result = obu('cdp', '--tab-id', str(tab), '--method', method, '--params', json.dumps(params))
    if result.get('exceptionDetails'):
        raise RuntimeError('Page evaluation failed: ' + json.dumps(result['exceptionDetails']))
    return result

def page_value(tab):
    result = cdp(tab, 'Runtime.evaluate', {'expression': '({note:document.querySelector("#note")?.value,progress:document.body.dataset.progress,cookie:document.cookie.split("; ").find(value=>value.startsWith("resident-proof="))})', 'returnByValue': True})
    return result['result']['value']

def download_value(path):
    source = 'import json,pathlib,sys; p=pathlib.Path(sys.argv[1]); print(json.dumps(p.read_text() if p.is_file() else None))'
    return json.loads(run(['exec', worker, '/usr/bin/python3', '-c', source, path]))

def editor_value(pid, window):
    state = cua('get_window_state', {'pid': pid, 'window_id': window, 'include_screenshot': False})
    return state, next((element for element in state['elements'] if element.get('role') == 'text'), None)

def process_identities(editor_pid):
    # Names identify diagnostic candidates only. Fresh kernel parentage binds
    # each observed service/application to the exact managed resident root.
    source = r'''
import json,os,pathlib,sys
rows={}
for path in pathlib.Path('/proc').iterdir():
 if not path.name.isdigit():continue
 try:
  raw=(path/'stat').read_text(); end=raw.rfind(')'); fields=raw[end+2:].split()
  rows[int(path.name)]={'pid':int(path.name),'parent':int(fields[1]),'start':fields[19],'state':fields[0],'comm':raw[raw.find('(')+1:end]}
 except FileNotFoundError:pass
roots=[]
for pid,row in rows.items():
 if row['parent']!=1:continue
 try:args=(pathlib.Path('/proc')/str(pid)/'cmdline').read_bytes().split(b'\0')
 except (FileNotFoundError,PermissionError):continue
 if args[1:4]==[b'init',b'worker',b'desktop']:roots.append(pid)
if len(roots)!=1:raise RuntimeError('Exact resident init root unavailable')
root=roots[0]
def resident(pid,ancestor=root):
 for unused in range(len(rows)):
  if pid==ancestor:return True
  if pid<=1 or pid not in rows:return False
  pid=rows[pid]['parent']
 return False
result={'resident-root':rows[root]}
for label,comm in [('Supervisor','supervisord'),('Cua','cua-driver'),('Xvfb','Xvfb'),('D-Bus','dbus-daemon')]:
 matches=[row for row in rows.values() if row['comm']==comm and resident(row['pid']) and (label=='Supervisor' or row['parent']==result['Supervisor']['pid'])]
 if len(matches)!=1:raise RuntimeError('Resident diagnostic identity is ambiguous: '+label)
 result[label]=matches[0]
chrome=[]
for pid,row in rows.items():
 if row['comm']!='chrome' or row['state'] in ('Z','X') or not resident(pid):continue
 try:args=(pathlib.Path('/proc')/str(pid)/'cmdline').read_bytes().split(b'\0')
 except (FileNotFoundError,PermissionError):continue
 if args[0] and not any(arg.startswith(b'--type=') or b' --type=' in arg for arg in args):chrome.append(row)
if len(chrome)!=1:raise RuntimeError('Resident main Chrome identity is ambiguous: '+json.dumps(chrome))
result['Chrome']=chrome[0]
editor=int(sys.argv[1])
if editor not in rows or not resident(editor):raise RuntimeError('Native editor is outside the resident root')
result['native-editor']=rows[editor]
for label,row in result.items():
 raw=(pathlib.Path('/proc')/str(row['pid'])/'environ').read_bytes().split(b'\0')
 row['environment']={key:value for entry in raw if b'=' in entry for key,value in [entry.decode().split('=',1)] if key in ('HOME','XDG_CONFIG_HOME','CHROME_CONFIG_HOME','DISPLAY','XAUTHORITY','XDG_RUNTIME_DIR')}
for label in ('Cua','native-editor'):
 if result[label]['environment'].get('HOME')!=sys.argv[3]:raise RuntimeError(label+' lost its neutral desktop HOME')
browser=result['Chrome']
if os.readlink('/proc/'+str(browser['pid'])+'/exe')!=sys.argv[4]:raise RuntimeError('Resident Chrome is not the installed Stable executable')
args=(pathlib.Path('/proc')/str(browser['pid'])/'cmdline').read_bytes().split(b'\0')
if any(arg.startswith((b'--user-data-dir',b'--profile-directory')) for arg in args):raise RuntimeError('Chrome selected another profile through launch arguments')
profile=pathlib.Path(sys.argv[2])
default=profile/'Default'
# Chrome hides some process environments. Bind every observable witness to
# the actual Chrome tree, and require a witness with the effective settings.
witnesses=[]
for pid in rows:
 if not resident(pid,browser['pid']):continue
 try:raw=(pathlib.Path('/proc')/str(pid)/'environ').read_bytes().split(b'\0')
 except (FileNotFoundError,PermissionError):continue
 values={key:value for entry in raw if b'=' in entry for key,value in [entry.decode().split('=',1)] if key in ('HOME','XDG_CONFIG_HOME','CHROME_CONFIG_HOME')}
 if not values:continue
 if values.get('HOME')!=sys.argv[3] or values.get('XDG_CONFIG_HOME')!=str(profile.parent):raise RuntimeError('Chrome did not retain neutral HOME with its original XDG configuration')
 config=values.get('CHROME_CONFIG_HOME') or values['XDG_CONFIG_HOME']
 if str(profile)!=config+'/google-chrome':raise RuntimeError('Chrome selected another configuration directory')
 witnesses.append({'pid':pid,'start':rows[pid]['start'],'environment':values})
if not witnesses:raise RuntimeError('No kernel-bound Chrome environment witness is available')
browser['environmentWitnesses']=witnesses
if default.resolve()!=default:raise RuntimeError('Resident Chrome did not use the original image directory')
opened=set()
for pid in rows:
 if not resident(pid,browser['pid']):continue
 for fd in (pathlib.Path('/proc')/str(pid)/'fd').glob('*'):
  try:target=os.readlink(fd)
  except (FileNotFoundError,PermissionError):continue
  if target.startswith(str(profile)+'/Default/'):opened.add(target)
if not opened:raise RuntimeError('No Chrome descendant holds the original Default profile open')
stat=profile.stat()
browser['userDataDirectory']={'path':str(profile),'device':stat.st_dev,'inode':stat.st_ino,'defaultInode':default.stat().st_ino,'openFiles':sorted(opened)}
print(json.dumps(result))
'''
    return json.loads(run(['exec', worker, '/usr/bin/python3', '-c', source, str(editor_pid), binding['chromeUserDataDir'], binding['desktopHome'], binding['chromeExecutable']]))

# The admitted worker uses a writable image layer with its prepared HOME.
security = ['--user', '65532:65532', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--security-opt', 'seccomp=unconfined']
try:
    run(['network', 'create', '--internal'] + resource_label + [network])
    run(['volume', 'create'] + resource_label + [private_volume])
    workspace_mount = f'type=bind,source={workspace},target=/workspace'
    nfs_server = '127.0.0.1'
    if use_nfs:
        chart = env.get('MULTICA_RESIDENT_CHART', str(pathlib.Path(source_root).parent / 'helm/charts/multica-runtime-controller'))
        fixture = proof / 'nfs-values.json'
        fixture.write_text(json.dumps({'controller': {'storage': {'size': '2Gi', 'maxBytes': 1 << 30}},
                                       'nfs': {'trustedNodeCIDRs': ['192.0.2.1/32']},
                                       'networkPolicy': {'blockKubernetesAPI': False}}))
        rendered = subprocess.run(['helm', 'template', token, chart, '--values', str(fixture)],
                                  env=env, text=True, capture_output=True, check=True).stdout
        config = subprocess.run(['yq', '-o=json', '-I=0'], input=rendered,
                                env=env, text=True, capture_output=True, check=True).stdout
        docs = [json.loads(line) for line in config.splitlines() if line.strip() and line != '---']
        nfs_config = next(doc['data'] for doc in docs if doc['kind'] == 'ConfigMap' and 'ganesha.conf' in doc.get('data', {}))
        deployment = next(doc for doc in docs if doc['kind'] == 'Deployment')
        nfs_container = next(row for row in deployment['spec']['template']['spec']['containers'] if row['name'] == 'nfs')
        config_dir = proof / 'nfs-config'
        config_dir.mkdir()
        for name, value in nfs_config.items():
            (config_dir / name).write_text(value)
        for volume in (workspace_volume, recovery_volume):
            run(['volume', 'create'] + resource_label + [volume]); volumes.append(volume)
        workspace_mount = f'type=volume,source={workspace_volume},target=/workspace'
        run(['run', '--rm', '--network', 'none', '--user', '0:0', '--cap-drop', 'ALL', '--cap-add', 'CHOWN',
             '--mount', workspace_mount, '--entrypoint', '/bin/chown', image, '65532:65532', '/workspace'])
        run(['run', '-d', '--name', nfs, '--network', network, '--user', '0:0', '--cap-drop', 'ALL'] + resource_label +
            [option for cap in nfs_container['securityContext']['capabilities']['add'] for option in ('--cap-add', cap)] +
            ['--security-opt', 'seccomp=unconfined', '--tmpfs', '/run/ganesha:rw,mode=0755', '--mount', workspace_mount,
             '--mount', f'type=volume,source={recovery_volume},target=/var/lib/nfs/ganesha',
             '--mount', f'type=bind,source={config_dir},target=/etc/ganesha,readonly',
             '--entrypoint', nfs_container['command'][0], nfs_container['image']] + nfs_container['args'])
        nfs_server = json.loads(run(['inspect', '--format', '{{json .NetworkSettings.Networks}}', nfs]))[network]['IPAddress']
        deadline = time.monotonic() + 30
        while True:
            probe = subprocess.run(docker + ['exec', nfs, '/usr/bin/timeout', '3', '/bin/bash', '/etc/ganesha/ready.sh'],
                                   env=env, text=True, capture_output=True, timeout=5)
            if probe.returncode == 0: break
            if not json.loads(run(['inspect', '--format', '{{json .State}}', nfs]))['Running'] or time.monotonic() >= deadline:
                raise RuntimeError('NFS export unavailable: ' + run(['logs', nfs])[-3000:])
            time.sleep(.2)
        (proof / 'nfs-rendered.yaml').write_text(rendered)
    # Match kubelet's fsGroup setup. Unix sockets require native Linux storage.
    run(['run', '--rm', '--network', 'none', '--read-only', '--user', '0:0', '--cap-drop', 'ALL', '--cap-add', 'CHOWN',
         '--security-opt', 'no-new-privileges', '--mount', f'type=volume,source={private_volume},target=/opt/multica/private',
         '--entrypoint', '/bin/chown', image, '65532:65532', '/opt/multica/private'])
    run(['run', '-d', '--name', controller, '--network', network, '--network-alias', 'resident-controller'] + resource_label + security +
        ['--mount', f'type=bind,source={proof},target=/proof,readonly',
         '--mount', f'type=bind,source={control},target=/fixture-control',
         '--mount', workspace_mount,
         '--tmpfs', '/tmp:rw,exec,mode=1777',
         '--env', 'MULTICA_RESIDENT_DESKTOP_PROOF=/fixture-control', '--env', 'MULTICA_RESIDENT_IMAGE_ID=' + image,
         '--env', 'MULTICA_RESIDENT_NFS_SERVER=' + nfs_server,
         '--env', 'MULTICA_RESIDENT_NFS_ROOT_LOSS=' + ('1' if root_loss else '0'),
         '--entrypoint', '/proof/controller.test', image, '-test.v', '-test.timeout=390s', '-test.run', '^TestResidentDesktopGraphicalController$'])
    binding = json.loads(wait_file(control / 'bootstrap.json'))
    (proof / 'binding.json').write_text(json.dumps(binding, indent=2))
    # Inspect the untouched image. No profile provisioning runs in the fixture.
    source = r'''
import hashlib,json,pathlib,sys
profile=pathlib.Path(sys.argv[1])
paths=[profile,profile/'Default',profile/'Default'/'Preferences']
rows={}
for path in paths:
 if path.resolve()!=path:raise RuntimeError('Prepared Chrome profile is a link')
 stat=path.stat()
 if stat.st_uid!=65532 or stat.st_gid!=65532:raise RuntimeError('Prepared Chrome profile has the wrong owner')
 rows[str(path)]={'uid':stat.st_uid,'gid':stat.st_gid,'mode':oct(stat.st_mode&0o777),'bytes':stat.st_size}
raw=paths[-1].read_bytes()
settings=json.loads(raw).get('extensions',{}).get('settings',{})
if not any(value.get('manifest',{}).get('name')=='Open Browser Use' and not value.get('disable_reasons') for value in settings.values()):raise RuntimeError('Original image Default lacks its enabled Open Browser Use extension')
print(json.dumps({'paths':rows,'preferencesSHA256':hashlib.sha256(raw).hexdigest()}))
'''
    baseline = json.loads(run(['run', '--rm', '--network', 'none'] + security +
        ['--read-only', '--entrypoint', '/usr/bin/python3', image, '-c', source, binding['chromeUserDataDir']]))
    (proof / 'image-chrome-profile.json').write_text(json.dumps(baseline, indent=2))
    request_mount = ['--mount', f'type=bind,source={control / "request.json"},target=/etc/multica/task/request.json,readonly']
    run(['run', '--rm', '--name', layout, '--network', network] + resource_label + security + request_mount +
        ['--mount', f'type=volume,source={private_volume},target=/opt/multica/private', '--entrypoint', '/opt/multica/controller/runtime', image,
         'worker', 'layout', '--private-root=/opt/multica/private'], timeout=60)
    task_root = binding['taskRoot']
    host_task = workspace / task_root.removeprefix('/workspace/')
    task_mount = f'type=bind,source={host_task},target={task_root}'
    if use_nfs:
        run(['volume', 'create', '--driver', 'local'] + resource_label + ['--opt', 'type=nfs',
             '--opt', 'o=addr=' + nfs_server + ',vers=4.0,proto=tcp,hard,rw',
             '--opt', 'device=:' + task_root, task_volume])
        volumes.append(task_volume)
        task_mount = f'type=volume,source={task_volume},target={task_root}'
    run(['run', '-d', '--name', worker, '--network', network] + resource_label + security + request_mount +
        ['--mount', task_mount,
         '--mount', f'type=volume,source={private_volume},volume-subpath=run,target=/run/multica',
         '--mount', f'type=volume,source={private_volume},volume-subpath=tmp,target=/tmp',
         '--shm-size', '512m', '--env', 'POD_UID=' + binding['podUID'],
         '--env', 'MULTICA_REQUEST_DIGEST=' + binding['requestDigest'], image, 'worker', 'serve'])
    worker_started = True
    wait_file(host_task / 'workdir/proof-turn-1')
    if use_nfs:
        source = 'import json,pathlib,sys; print(json.dumps([line for line in pathlib.Path("/proc/self/mountinfo").read_text().splitlines() if line.split()[4]==sys.argv[1]]))'
        mountinfo = json.loads(run(['exec', worker, '/usr/bin/python3', '-c', source, task_root]))
        (proof / 'nfs-task-mount.json').write_text(json.dumps({'server': nfs_server, 'taskRoot': task_root, 'mountinfo': mountinfo}))
        if len(mountinfo) != 1:
            raise RuntimeError('The task has no unique scoped NFS mount')
        mounted = mountinfo[0].split(' - ', 1)[1].split()
        options = dict(item.split('=', 1) for item in mounted[2].split(',') if '=' in item)
        if mounted[0] not in ('nfs', 'nfs4') or mounted[1] not in (':' + task_root, nfs_server + ':' + task_root) or options.get('addr') != nfs_server:
            raise RuntimeError('The worker mounted a different filesystem, server, or task export')
        # The installed SDK writes this file through NFS; controller reads the
        # exact shared backing tree. Keep the normal native/browser oracles.
        (proof / 'nfs-turn-1.json').write_text(json.dumps({'marker': read_file(host_task / 'workdir/proof-turn-1')}))
    # The current authenticated SDK turn already ran the supported background
    # Chrome startup. Allow the extension its full reconnect window.
    deadline, connected = time.monotonic() + 60, False
    for _ in range(30):
        time.sleep(2)
        if time.monotonic() >= deadline: break
        try:
            if obu('ping', '--timeout', '1s') == 'pong': connected = True; break
        except (RuntimeError, subprocess.TimeoutExpired):
            pass
    if not connected:
        raise RuntimeError('Open Browser Use did not connect after the supported startup and 60-second recovery window')
    browser_instance = obu('info')['metadata']['extensionInstanceId']
    obu('tabs')
    obu('name-session', '--name', 'Resident desktop proof - OBU')
    opened = obu('open-tab', '--url', 'http://resident-controller:8081/page')
    tabs = obu('tabs')
    tab = opened.get('id', opened.get('tabId'))
    if tab is None:
        tab = next(t['id'] for t in tabs if t.get('url') == 'http://resident-controller:8081/page')
    if not any(t.get('id', t.get('tabId')) == tab for t in tabs): raise RuntimeError('New browser tab is outside the task session')
    (proof / 'browser-session.json').write_text(json.dumps({'sessionID': session, 'extensionInstanceID': browser_instance, 'tabID': tab}))
    if 'note' not in page_value(tab): raise RuntimeError('The owned proof page has no form input')
    cdp(tab, 'Runtime.evaluate', {'expression': 'document.querySelector("#note").value="retained native tab"', 'returnByValue': True})
    cookie = 'resident-proof=' + token
    cdp(tab, 'Runtime.evaluate', {'expression': 'document.cookie=' + json.dumps(cookie + '; Path=/; SameSite=Lax'), 'returnByValue': True})
    if page_value(tab).get('cookie') != cookie: raise RuntimeError('The local browser cookie was not accepted')
    cdp(tab, 'Runtime.evaluate', {'expression': 'document.querySelector("#download").click()', 'returnByValue': True})
    editor = json.loads(wait_file(host_task / 'workdir/proof-editor.json'))
    pid, window = editor['pid'], editor['windows'][0]['window_id']
    state, text = editor_value(pid, window)
    if text is None: raise RuntimeError('Native editor has no editable text')
    cua('type_text', {'target': {'kind': 'window', 'pid': pid, 'window_id': window}, 'element_token': text['element_token'], 'text': 'retained unsaved document'})
    state, text = editor_value(pid, window)
    if text.get('value', text.get('label')) != 'retained unsaved document': raise RuntimeError('Native editor did not retain typed text')
    first_identities = process_identities(pid)
    (proof / 'first-process-identities.json').write_text(json.dumps(first_identities, indent=2))
    for name in ('download-accepted', 'slow-accepted', 'idle-accepted'):
        wait_file(control / name)
    (control / 'finish-1').touch()
    wait_file(control / 'idle-1.json')
    wait_file(host_task / 'workdir/application-idle-write', timeout=30)
    download = binding['desktopHome'] + '/Downloads/resident-proof.txt'
    deadline = time.monotonic() + 30
    while page_value(tab).get('progress', '').strip() != 'progress-during-idle' or download_value(download) != 'accepted-before-idle\nprogress-during-idle\n':
        if time.monotonic() >= deadline:
            source = 'import json,pathlib,sys; print(json.dumps({base:[p.name for p in pathlib.Path(base).glob("*")] for base in sys.argv[1:]}))'
            paths = run(['exec', worker, '/usr/bin/python3', '-c', source, binding['desktopHome'] + '/Downloads', '/home/multica/agents/Downloads', task_root + '/workdir'])
            raise RuntimeError('Accepted idle activity did not finish: ' + json.dumps({'page': page_value(tab), 'download': download_value(download), 'directories': json.loads(paths)}))
        time.sleep(.2)
    task_bytes = read_file(host_task / 'workdir/task-writer')
    (control / 'next-turn').touch()
    wait_file(host_task / 'workdir/proof-turn-2')
    if obu('info')['metadata']['extensionInstanceId'] != browser_instance: raise RuntimeError('Native browser extension connection changed')
    tabs = obu('tabs')
    if not any(t.get('id', t.get('tabId')) == tab for t in tabs): raise RuntimeError('The first browser tab disappeared')
    if page_value(tab)['note'] != 'retained native tab': raise RuntimeError('Native tab state restarted')
    if page_value(tab).get('cookie') != cookie: raise RuntimeError('The local browser cookie was lost between turns')
    if download_value(download) != 'accepted-before-idle\nprogress-during-idle\n': raise RuntimeError('The accepted download did not survive into the next turn')
    state, text = editor_value(pid, window)
    if text.get('value', text.get('label')) != 'retained unsaved document': raise RuntimeError('The unsaved native buffer was lost')
    if read_file(host_task / 'workdir/task-writer') != task_bytes: raise RuntimeError('An old task writer survived settlement')
    second_identities = process_identities(pid)
    (proof / 'second-process-identities.json').write_text(json.dumps(second_identities, indent=2))
    first_turn = json.loads((control / 'idle-1.json').read_text())
    if binding['podUID'] != first_turn['podUID'] or first_turn['podCreates'] != 1:
        raise RuntimeError('The first turn created an extra worker incarnation')
    (proof / 'initialization.json').write_text(json.dumps({
        'workerContainer': json.loads(run(['inspect', '--format', '{{json .State}}', worker])),
        'firstTurn': json.loads((control / 'idle-1.json').read_text()),
        'taskLayoutContainer': layout, 'transport': 'nfs' if use_nfs else 'bind',
        'ChromeBefore': first_identities['Chrome']['pid'], 'ChromeAfter': second_identities['Chrome']['pid'],
        'ChromeStartBefore': first_identities['Chrome']['start'], 'ChromeStartAfter': second_identities['Chrome']['start']}))
    if any(second_identities[name]['pid'] != row['pid'] or second_identities[name]['start'] != row['start'] for name,row in first_identities.items()):
        raise RuntimeError('A resident service or Chrome process restarted between native turns')
    if any(second_identities['Chrome']['userDataDirectory'][key] != first_identities['Chrome']['userDataDirectory'][key] for key in ('path','device','inode','defaultInode')):
        raise RuntimeError('Chrome changed its original user data directory between native turns')
    (control / 'finish-2').touch()
    wait_file(control / 'idle-2.json')
    second_turn = json.loads((control / 'idle-2.json').read_text())
    if second_turn['podUID'] != first_turn['podUID'] or second_turn['podCreates'] != first_turn['podCreates']:
        raise RuntimeError('The compatible turn created another worker incarnation')
    obu('tabs')
    obu('finalize-tabs', '--keep', '[]')
    browser_finalized = True
    if root_loss:
        # Delete only this invocation's settled synthetic workspace. Result and
        # control evidence remain in the independent fixture directory.
        source = 'import pathlib,shutil,sys; p=pathlib.Path(sys.argv[1]); assert p.resolve()==p and p.is_dir(); shutil.rmtree(p)'
        run(['exec', controller, '/usr/bin/python3', '-c', source, task_root])
        (proof / 'root-loss.json').write_text(json.dumps({'taskRoot': task_root, 'afterSettledTurn': second_turn}))
    (control / 'stop').touch()
    run(['wait', worker], timeout=binding['terminationGraceSeconds'] + 60 + 10)
    termination = json.loads(run(['inspect', '--format', '{{json .State}}', worker]))
    if termination['Running'] or termination['OOMKilled']: raise RuntimeError('The worker did not complete bounded shutdown')
    (control / 'termination.json').write_text(json.dumps({'PodUID': binding['podUID'], 'ExitCode': termination['ExitCode'], 'FinishedAt': termination['FinishedAt']}))
    run(['wait', controller], timeout=60)
    logs = run(['logs', controller])
    (proof / 'controller.log').write_text(logs)
    if not (control / 'complete.json').exists() or '\nPASS\n' not in '\n' + logs:
        raise RuntimeError('Controller did not prove final checkpoint after actual worker termination')
    print('Native two-turn graphical, active idle, task fence, accepted resume, and ' +
          ('dirty closure after root loss' if root_loss else 'clean final storage') + ' proof passed. Evidence: ' + str(proof))
finally:
    if browser_instance is not None and not browser_finalized:
        try:
            if obu('info')['metadata']['extensionInstanceId'] == browser_instance:
                obu('tabs')
                obu('finalize-tabs', '--keep', '[]')
        except Exception as error:
            print('Owned browser session cleanup: ' + str(error), file=sys.stderr)
    cleanup_errors = []
    for name in (layout, worker, controller, nfs) if use_nfs else (layout, worker, controller):
        observed = subprocess.run(docker + ['inspect', name], env=env, text=True, capture_output=True)
        if observed.returncode: continue
        owned = json.loads(observed.stdout)[0]
        if owned['Config'].get('Labels', {}).get('multica.proof') != token:
            cleanup_errors.append('Container identity changed: ' + name)
            continue
        try:
            logs = run(['logs', owned['Id']])
            (proof / (name.rsplit('-',1)[-1] + '.log')).write_text(logs)
        except Exception: pass
        result = subprocess.run(docker + ['rm', '-f', owned['Id']], env=env, text=True, capture_output=True)
        if result.returncode: cleanup_errors.append('Container cleanup incomplete: ' + name + ': ' + result.stderr)
    result = subprocess.run(docker + ['network', 'rm', network], env=env, text=True, capture_output=True)
    if result.returncode: cleanup_errors.append('Network cleanup incomplete: ' + result.stderr)
    for volume in reversed(volumes):
        result = subprocess.run(docker + ['volume', 'rm', volume], env=env, text=True, capture_output=True)
        if result.returncode: cleanup_errors.append('Owned volume cleanup incomplete: ' + volume + ': ' + result.stderr)
    (proof / 'cleanup.json').write_text(json.dumps({'token': token, 'errors': cleanup_errors}))
    if cleanup_errors: raise RuntimeError('\n'.join(cleanup_errors))
PY
