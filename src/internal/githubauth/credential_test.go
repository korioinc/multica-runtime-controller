package githubauth

import (
	"strings"
	"testing"
)

func TestCredentialTargetCannotChangeHostOrRepositoryBoundary(t *testing.T) {
	for _, input := range []string{
		"protocol=https\nhost=other.example\npath=team/repository.git\n\n",
		"protocol=https\nhost=github.com\nhost=other.example\npath=team/repository.git\n\n",
		"protocol=https\nhost=github.com\npath=team/repository.git/../another.git\n\n",
		"protocol=https\nhost=github.com\npath=team/repository.git%0apassword=identity\n\n",
		"protocol=https\nhost=github.com\npath=team/repository.git\r\n\n",
		"protocol=https\nhost=github.com\n\n",
	} {
		if _, err := credentialRepository(strings.NewReader(input)); err == nil {
			t.Fatal("Git input escaped the single GitHub repository credential boundary")
		}
	}
	if _, err := credentialRepository(strings.NewReader("protocol=https\nhost=github.com\npath=team/selected.git\n\n")); err != nil {
		t.Fatal("Git could not request the explicitly selected repository credential")
	}
}
