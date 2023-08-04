package goreplay

import "fmt"

type ProtoPaths []string

func (p *ProtoPaths) String() string {
	return fmt.Sprint(*p)
}

func (p *ProtoPaths) Set(value string) error {
	*p = append(*p, value)
	return nil
}
