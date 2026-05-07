package memoize

type Params struct {
	args map[string]any
}

func NoParams() Params {
	return Params{}
}

func NewParams(args map[string]any) *Params {
	return &Params{args: args}
}

func (p *Params) Set(key string, value any) {
	if p.args == nil {
		p.args = make(map[string]any)
	}
	p.args[key] = value
}

func (p *Params) Get(key string) any {
	if p.args == nil {
		return nil
	}
	return p.args[key]
}
