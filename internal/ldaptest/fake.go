package ldaptest

import (
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-ldap/ldap/v3"
	msg "github.com/vjeantet/goldap/message"
	"github.com/vjeantet/ldapserver"
)

// Operation names accepted by Fake.Count. They are declared up front so later
// handlers only have to be registered, not named.
const (
	// OpBind counts bind requests.
	OpBind = "bind"
	// OpSearch counts search requests.
	OpSearch = "search"
	// OpModify counts modify requests.
	OpModify = "modify"
	// OpDelete counts delete requests.
	OpDelete = "delete"
	// OpWhoAmI counts "who am I?" extended requests.
	OpWhoAmI = "whoami"
)

// Search scopes as encoded on the wire (RFC 4511 section 4.5.1).
const (
	scopeBase = 0
	scopeOne  = 1
	scopeSub  = 2
)

// entry is a stored directory entry.
type entry struct {
	dn    string
	key   string // normalised dn
	attrs map[string][]string
}

// ModifyChange is one change of a modify request exactly as the server received
// it.
type ModifyChange struct {
	Op   int      // 0 add, 1 delete, 2 replace (RFC 4511 section 4.6)
	Attr string   // attribute description as sent
	Vals [][]byte // raw attribute values as sent
}

// ModifyRecord is one modify request exactly as the server received it.
type ModifyRecord struct {
	DN      string // target DN as sent, not normalised
	Changes []ModifyChange
}

// connState is the per-connection state of a live client connection.
type connState struct {
	seq      int    // accept order, starting at 1
	identity string // DN of the last successful bind; empty while anonymous
}

// Behavior is the fault the fake injects into one request. The zero value is
// normal service.
type Behavior struct {
	// Delay is how long the server waits before applying the operation.
	Delay time.Duration
	// Hang makes the server never respond. The handler blocks until the request
	// is abandoned (the client connection ended) or the fake stops.
	Hang bool
	// CloseBefore closes the server-side connection on receipt, applying nothing
	// and answering nothing.
	CloseBefore bool
	// CloseAfter applies the operation, then closes the server-side connection
	// without answering ("apply, then drop").
	CloseAfter bool
}

// Fake is an in-process LDAP server speaking the real wire protocol. It serves
// bind, search, modify and delete from a small in-memory directory, counts every
// request it receives and records what modify and delete carried.
type Fake struct {
	ln  net.Listener
	srv *ldapserver.Server

	behavior atomic.Pointer[func(op string) Behavior] // fault hook; nil means none

	mu      sync.Mutex // guards following
	users   map[string]string
	entries []entry
	counts  map[string]int
	modlog  []ModifyRecord
	deleted []string
	conns   map[net.Conn]*connState // live client connections
	accepts int                     // connections accepted so far

	stop     chan struct{} // closed by Stop; handlers that block must select on it
	stopped  chan struct{} // closed when Serve returns
	stopOnce sync.Once
}

// acceptSignal wraps a listener so the first Accept closes entered. Waiting on
// it gives Stop a happens-before edge with the server's own assignment of its
// listener inside Serve.
type acceptSignal struct {
	net.Listener
	once    sync.Once
	entered chan struct{}
}

func (a *acceptSignal) Accept() (net.Conn, error) {
	a.once.Do(func() { close(a.entered) })
	c, err := a.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &fixedConn{Conn: c}, nil
}

// fixedConn repairs the message ID of every LDAP message the server writes.
//
// The goldap encoder used by the server library writes an INTEGER of 128..255
// (and 32768..65535, and so on) without the leading zero byte that keeps it
// positive, so a client decodes message ID 133 as -123 and drops the response.
// A connection therefore stops working after 127 requests. The server writes
// each LDAP message with exactly one Write call (it flushes after every
// message), so each Write can be repaired on its own.
type fixedConn struct {
	net.Conn
}

func (c *fixedConn) Write(p []byte) (int, error) {
	if _, err := c.Conn.Write(fixMessageID(p)); err != nil {
		return 0, err
	}
	return len(p), nil
}

// fixMessageID returns the LDAP message p with its message ID re-encoded as a
// non-negative INTEGER. Message IDs are never negative (RFC 4511 section 4.1.1),
// so an ID whose first content byte has the high bit set is the encoder bug.
// p is returned unchanged when it is well formed or not recognised.
func fixMessageID(p []byte) []byte {
	if len(p) < 2 || p[0] != 0x30 { // outer SEQUENCE
		return p
	}
	hdr, length := 2, int(p[1])
	if p[1]&0x80 != 0 {
		n := int(p[1] & 0x7f)
		if n == 0 || n > 4 || len(p) < 2+n {
			return p
		}
		hdr, length = 2+n, 0
		for _, b := range p[2:hdr] {
			length = length<<8 | int(b)
		}
	}
	if len(p) != hdr+length || len(p) < hdr+3 || p[hdr] != 0x02 { // INTEGER
		return p
	}
	idLen := int(p[hdr+1])
	if idLen >= 0x80 || len(p) < hdr+2+idLen || idLen == 0 || p[hdr+2]&0x80 == 0 {
		return p
	}
	body := make([]byte, 0, length+1)
	body = append(body, 0x02, byte(idLen+1), 0x00)
	body = append(body, p[hdr+2:]...)

	out := []byte{0x30}
	if n := len(body); n < 0x80 {
		out = append(out, byte(n))
	} else if n < 0x100 {
		out = append(out, 0x81, byte(n))
	} else if n < 0x10000 {
		out = append(out, 0x82, byte(n>>8), byte(n))
	} else {
		out = append(out, 0x83, byte(n>>16), byte(n>>8), byte(n))
	}
	return append(out, body...)
}

// NewFake serves LDAP on ln and returns once the server is accepting. The Fake
// takes ownership of ln; call Stop to release it.
func NewFake(ln net.Listener) *Fake {
	f := &Fake{
		ln:      ln,
		srv:     ldapserver.NewServer(),
		users:   make(map[string]string),
		counts:  make(map[string]int),
		conns:   make(map[net.Conn]*connState),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	mux := ldapserver.NewRouteMux()
	mux.Bind(f.route(OpBind, f.doBind))
	mux.Search(f.route(OpSearch, f.doSearch))
	mux.Modify(f.route(OpModify, f.doModify))
	mux.Delete(f.route(OpDelete, f.doDelete))
	mux.Extended(f.route(OpWhoAmI, f.doWhoAmI)).RequestName(ldapserver.NoticeOfWhoAmI)
	f.srv.Handle(mux)
	// Per-connection state is keyed by the net.Conn, never by the server
	// library's single per-client data field: request goroutines of one client
	// run concurrently and would race on that single field. m.Client.GetConn()
	// returns the same net.Conn these hooks receive.
	f.srv.OnNewConnection = f.onConnect
	f.srv.OnClientClose = f.onClose

	sig := &acceptSignal{Listener: ln, entered: make(chan struct{})}
	go func() {
		defer close(f.stopped)
		_ = f.srv.Serve(sig)
	}()
	<-sig.entered
	return f
}

// SetBehavior installs a fault hook. Every request asks h, once, for the
// Behavior to apply to the operation of that name (an Op* constant); the hook
// runs after the request has been counted and must not call back into the Fake.
// A nil h clears the hook. It is safe for concurrent use.
func (f *Fake) SetBehavior(h func(op string) Behavior) {
	if h == nil {
		f.behavior.Store(nil)
		return
	}
	f.behavior.Store(&h)
}

// route is the single handler seam. Per request it counts on receipt, reads the
// fault behaviour once, then closes, hangs or delays as asked, runs the op
// function (which applies the operation and returns the protocol ops to send)
// and finally writes the ops or, for CloseAfter, drops the connection instead.
// Op functions never touch the response writer themselves. mu is never held
// across a close, a wait or a write.
func (f *Fake) route(name string, op func(*ldapserver.Message) []msg.ProtocolOp) ldapserver.HandlerFunc {
	return func(w ldapserver.ResponseWriter, m *ldapserver.Message) {
		f.mu.Lock()
		f.counts[name]++
		f.mu.Unlock()

		var b Behavior
		if h := f.behavior.Load(); h != nil {
			b = (*h)(name)
		}
		if b.CloseBefore {
			_ = m.Client.GetConn().Close()
			return
		}
		if b.Hang {
			select {
			case <-m.Done:
			case <-f.stop:
			}
			return
		}
		if b.Delay > 0 {
			t := time.NewTimer(b.Delay)
			select {
			case <-t.C:
			case <-m.Done:
				t.Stop()
				return
			case <-f.stop:
				t.Stop()
				return
			}
		}
		ops := op(m)
		if b.CloseAfter {
			_ = m.Client.GetConn().Close()
			return
		}
		for _, po := range ops {
			w.Write(po)
		}
	}
}

// Addr returns the listener's host:port.
func (f *Fake) Addr() string { return f.ln.Addr().String() }

// URL returns the ldap:// URL of the fake.
func (f *Fake) URL() string { return "ldap://" + f.Addr() }

// Dial opens a real go-ldap connection to the fake. The caller closes it.
func (f *Fake) Dial() (*ldap.Conn, error) { return ldap.DialURL(f.URL()) }

// Count returns how many requests of the named operation the fake has received.
func (f *Fake) Count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[op]
}

// AddUser registers a DN and password that Bind accepts.
func (f *Fake) AddUser(dn, password string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.users[normDN(dn)] = password
}

// AddEntry appends a directory entry that Search can return.
func (f *Fake) AddEntry(dn string, attrs map[string][]string) {
	cp := make(map[string][]string, len(attrs))
	for k, v := range attrs {
		cp[k] = append([]string(nil), v...)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, entry{dn: dn, key: normDN(dn), attrs: cp})
}

// onConnect registers state for a newly accepted connection. It runs before
// the connection's first request is read.
func (f *Fake) onConnect(c net.Conn) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accepts++
	f.conns[c] = &connState{seq: f.accepts}
	return nil
}

// onClose forgets a connection whose session ended. It must not block.
func (f *Fake) onClose(c net.Conn, _ any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.conns, c)
}

// setIdentity records the bound identity of c. A connection that is already
// gone is ignored.
func (f *Fake) setIdentity(c net.Conn, identity string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if st := f.conns[c]; st != nil {
		st.identity = identity
	}
}

// identityOf returns the bound identity of c, empty while anonymous.
func (f *Fake) identityOf(c net.Conn) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if st := f.conns[c]; st != nil {
		return st.identity
	}
	return ""
}

// Identities returns the bound identity of every live connection in accept
// order. An empty string means the connection is anonymous.
func (f *Fake) Identities() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	states := make([]*connState, 0, len(f.conns))
	for _, st := range f.conns {
		states = append(states, st)
	}
	sort.Slice(states, func(i, j int) bool { return states[i].seq < states[j].seq })
	out := make([]string, len(states))
	for i, st := range states {
		out[i] = st.identity
	}
	return out
}

// NumConns returns the number of client connections the server currently holds.
func (f *Fake) NumConns() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.conns)
}

// CloseConns closes every server-side connection, so each client sees the
// socket close. The server's session teardown then removes them from NumConns.
func (f *Fake) CloseConns() {
	f.mu.Lock()
	conns := make([]net.Conn, 0, len(f.conns))
	for c := range f.conns {
		conns = append(conns, c)
	}
	f.mu.Unlock()
	// Closing triggers the client-close hook, which takes mu: close only after
	// releasing it.
	for _, c := range conns {
		_ = c.Close()
	}
}

// ModifyLog returns every modify request the server received, in arrival order,
// exactly as sent. Requests for unknown DNs are included. The result is a copy.
func (f *Fake) ModifyLog() []ModifyRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ModifyRecord, len(f.modlog))
	for i, r := range f.modlog {
		out[i] = ModifyRecord{DN: r.DN, Changes: make([]ModifyChange, len(r.Changes))}
		for j, c := range r.Changes {
			vals := make([][]byte, len(c.Vals))
			for k, v := range c.Vals {
				vals[k] = append([]byte(nil), v...)
			}
			out[i].Changes[j] = ModifyChange{Op: c.Op, Attr: c.Attr, Vals: vals}
		}
	}
	return out
}

// Deleted returns the DN of every delete request the server received, in
// arrival order, exactly as sent. Requests for unknown DNs are included.
func (f *Fake) Deleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

// Entry returns a copy of the attributes of the stored entry named dn.
func (f *Fake) Entry(dn string) (map[string][]string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.indexLocked(normDN(dn))
	if i < 0 {
		return nil, false
	}
	cp := make(map[string][]string, len(f.entries[i].attrs))
	for k, v := range f.entries[i].attrs {
		cp[k] = append([]string(nil), v...)
	}
	return cp, true
}

// indexLocked returns the index of the entry with the normalised DN key, or -1.
func (f *Fake) indexLocked(key string) int {
	for i := range f.entries {
		if f.entries[i].key == key {
			return i
		}
	}
	return -1
}

// Stop shuts the server down and waits for it to return. It is idempotent.
func (f *Fake) Stop() {
	f.stopOnce.Do(func() {
		close(f.stop)
		f.srv.Stop()
		<-f.stopped
	})
}

// doBind answers a bind request. An empty DN with an empty password is an
// anonymous bind; a known DN with its password succeeds; everything else,
// including non-simple authentication, is invalid credentials (49). A success
// stores the DN as sent as the connection's identity; a failure clears it.
func (f *Fake) doBind(m *ldapserver.Message) []msg.ProtocolOp {
	r := m.GetBindRequest()
	code := ldapserver.LDAPResultInvalidCredentials
	identity := ""
	if r.AuthenticationChoice() == "simple" {
		dn, pw := string(r.Name()), string(r.AuthenticationSimple())
		f.mu.Lock()
		want, known := f.users[normDN(dn)]
		f.mu.Unlock()
		if (dn == "" && pw == "") || (known && want == pw) {
			code = ldapserver.LDAPResultSuccess
			identity = dn
		}
	}
	// A failed bind leaves the connection anonymous (RFC 4511 section 4.2.1),
	// so identity is stored unconditionally: empty on failure.
	f.setIdentity(m.Client.GetConn(), identity)
	res := ldapserver.NewBindResponse(code)
	if code != ldapserver.LDAPResultSuccess {
		res.SetDiagnosticMessage("invalid credentials")
	}
	return []msg.ProtocolOp{res}
}

// doWhoAmI answers a "who am I?" request with dn:<identity> for a bound
// connection and an empty authzId for an anonymous one (RFC 4532).
func (f *Fake) doWhoAmI(m *ldapserver.Message) []msg.ProtocolOp {
	value := ""
	if id := f.identityOf(m.Client.GetConn()); id != "" {
		value = "dn:" + id
	}
	resp, err := whoAmIResponse(value)
	if err != nil {
		return []msg.ProtocolOp{ldapserver.NewExtendedResponse(ldapserver.LDAPResultOperationsError)}
	}
	return []msg.ProtocolOp{resp}
}

// doModify records the request and applies it to the stored entry when the DN
// exists; an unknown DN yields no such object (32). The request is recorded
// either way because the server did see it.
func (f *Fake) doModify(m *ldapserver.Message) []msg.ProtocolOp {
	r := m.GetModifyRequest()
	rec := ModifyRecord{DN: string(r.Object())}
	changes := r.Changes()
	for i := range changes {
		mod := changes[i].Modification()
		c := ModifyChange{Op: int(changes[i].Operation()), Attr: string(mod.Type_())}
		for _, v := range mod.Vals() {
			c.Vals = append(c.Vals, []byte(string(v)))
		}
		rec.Changes = append(rec.Changes, c)
	}

	f.mu.Lock()
	f.modlog = append(f.modlog, rec)
	code := ldapserver.LDAPResultNoSuchObject
	if i := f.indexLocked(normDN(rec.DN)); i >= 0 {
		for _, c := range rec.Changes {
			applyChange(f.entries[i].attrs, c)
		}
		code = ldapserver.LDAPResultSuccess
	}
	f.mu.Unlock()
	return []msg.ProtocolOp{ldapserver.NewModifyResponse(code)}
}

// doDelete records the request and removes the entry when it exists; an
// unknown DN yields no such object (32).
func (f *Fake) doDelete(m *ldapserver.Message) []msg.ProtocolOp {
	dn := string(m.GetDeleteRequest())

	f.mu.Lock()
	f.deleted = append(f.deleted, dn)
	code := ldapserver.LDAPResultNoSuchObject
	if i := f.indexLocked(normDN(dn)); i >= 0 {
		f.entries = append(f.entries[:i], f.entries[i+1:]...)
		code = ldapserver.LDAPResultSuccess
	}
	f.mu.Unlock()
	return []msg.ProtocolOp{ldapserver.NewDeleteResponse(code)}
}

// applyChange applies one modify change to an attribute map. Attribute names
// match case-insensitively, as in LDAP; an unmatched name is stored as sent.
func applyChange(attrs map[string][]string, c ModifyChange) {
	name := c.Attr
	for k := range attrs {
		if strings.EqualFold(k, c.Attr) {
			name = k
			break
		}
	}
	vals := make([]string, len(c.Vals))
	for i, v := range c.Vals {
		vals[i] = string(v)
	}
	switch c.Op {
	case 0: // add
		attrs[name] = append(attrs[name], vals...)
	case 1: // delete: the listed values, or the whole attribute when none are listed
		if len(vals) == 0 {
			delete(attrs, name)
			return
		}
		var keep []string
		for _, have := range attrs[name] {
			if !containsString(vals, have) {
				keep = append(keep, have)
			}
		}
		if len(keep) == 0 {
			delete(attrs, name)
		} else {
			attrs[name] = keep
		}
	case 2: // replace: no values removes the attribute
		if len(vals) == 0 {
			delete(attrs, name)
		} else {
			attrs[name] = vals
		}
	}
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// doSearch returns every stored entry inside the request's base object and
// scope, with all stored attributes, followed by a success done message. The
// fake does not evaluate filters; it only counts the request.
func (f *Fake) doSearch(m *ldapserver.Message) []msg.ProtocolOp {
	r := m.GetSearchRequest()
	base := normDN(string(r.BaseObject()))
	scope := int(r.Scope())

	f.mu.Lock()
	var hits []entry
	for _, e := range f.entries {
		if inScope(e.key, base, scope) {
			hits = append(hits, e)
		}
	}
	f.mu.Unlock()

	ops := make([]msg.ProtocolOp, 0, len(hits)+1)
	for _, e := range hits {
		re := ldapserver.NewSearchResultEntry(e.dn)
		names := make([]string, 0, len(e.attrs))
		for name := range e.attrs {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			vals := make([]msg.AttributeValue, len(e.attrs[name]))
			for i, v := range e.attrs[name] {
				vals[i] = msg.AttributeValue(v)
			}
			re.AddAttribute(msg.AttributeDescription(name), vals...)
		}
		ops = append(ops, re)
	}
	return append(ops, ldapserver.NewSearchResultDoneResponse(ldapserver.LDAPResultSuccess))
}

// normDN compares DNs textually: spaces trimmed, lower-cased. It deliberately
// does not use go-ldap's parser, so strict parsing cannot hide fake behaviour.
func normDN(dn string) string {
	return strings.ToLower(strings.TrimSpace(dn))
}

// inScope reports whether the normalised entry DN key is within base and scope.
func inScope(key, base string, scope int) bool {
	switch scope {
	case scopeBase:
		return key == base
	case scopeOne:
		rest, ok := relativeTo(key, base)
		return ok && rest != "" && !hasUnescapedComma(rest)
	case scopeSub:
		_, ok := relativeTo(key, base)
		return ok
	}
	return false
}

// relativeTo returns the part of key above base. ok is false when key is not
// base or below it. The result is empty when key equals base.
func relativeTo(key, base string) (rest string, ok bool) {
	if base == "" {
		return key, true
	}
	if key == base {
		return "", true
	}
	if strings.HasSuffix(key, ","+base) {
		return strings.TrimSuffix(key, ","+base), true
	}
	return "", false
}

func hasUnescapedComma(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case ',':
			return true
		}
	}
	return false
}
