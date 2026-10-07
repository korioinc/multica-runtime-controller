package workspace

// BeginCheckout records the exact controller container before it touches task
// files. An interrupted writer remains reserved until its termination is proven.
func (s *Store) BeginCheckout(id string, process PreparationProcess) error {
	if !process.valid() {
		return invalid("checkout controller process")
	}
	return s.updateGrant(id, func(_ *registry, g *TaskGrant) error {
		if g.CheckoutClosed || g.CheckoutProcess != nil || g.Stop != nil || g.ExecutionRevoked || g.Prepared == nil ||
			(g.State != "ready" && g.State != "offered" && g.State != "starting" && g.State != "started") {
			return ErrConflict
		}
		g.CheckoutProcess = &process
		g.CheckoutNeedsFlush = true
		return nil
	})
}

// CompleteCheckout requires either a drained local operation or positive
// Kubernetes evidence that this exact recorded controller container stopped.
func (s *Store) CompleteCheckout(id string, process PreparationProcess) error {
	return s.updateGrant(id, func(_ *registry, g *TaskGrant) error {
		if g.CheckoutProcess == nil || *g.CheckoutProcess != process {
			return ErrConflict
		}
		g.CheckoutProcess = nil
		return nil
	})
}

// CloseCheckouts is an irreversible, attempt-scoped fence. It is persisted
// before the controller acknowledges that the worker can flush and seal.
func (s *Store) CloseCheckouts(id string) error {
	return s.updateGrant(id, func(_ *registry, g *TaskGrant) error {
		g.CheckoutClosed = true
		if g.WorkerSessionID != "" {
			g.ExecutionRevoked = true
		}
		return nil
	})
}

// CheckoutsFlushed records a successful controller-local syncfs. A worker's
// NFS flush cannot discharge this obligation for writes made on the server.
func (s *Store) CheckoutsFlushed(id string) error {
	return s.updateGrant(id, func(_ *registry, g *TaskGrant) error {
		if !g.CheckoutClosed || g.CheckoutProcess != nil {
			return ErrConflict
		}
		g.CheckoutNeedsFlush = false
		return nil
	})
}
