package main

import "math/rand"

type Node struct {
	key  int
	next *Node
}

// Permgen Floyd's random permutation generator
func Permgen(n, m int) <-chan int {
	var head *Node
	s := make(map[int]*Node) // initialize sequence S to empty
	for j := n - m + 1; j <= n; j++ {
		t := rand.Intn(j) + 1
		if p, ok := s[t]; !ok { // if T is not in S then
			// prefix T to S
			head = &Node{
				key:  t,
				next: head,
			}
			s[t] = head
		} else {
			// insert J in S after T
			curr := &Node{
				key:  j,
				next: p.next,
			}
			s[j] = curr
			p.next = curr
		}
	}

	ch := make(chan int)
	go func() {
		defer close(ch)
		for ; head != nil; head = head.next {
			ch <- head.key
		}
	}()

	return ch
}
