package golog

import (
	. "github.com/adrianuswarmenhoven/golog/term"
	. "github.com/adrianuswarmenhoven/golog/util"

	"bufio"
	"os"
	"sync"
)

// CanProveParallel returns true if goal can be proven from facts and clauses
// in the database.  Once a solution is found, it abandons other
// solutions (like once/1).
func (self *machine) CanProveParallel(goal interface{}) bool {
	var answer []Bindings
	var err error

	goalTerm := self.toGoal(goal)
	m := self.PushConj(goalTerm)
	for {
		m, answer, err = m.StepParallel()
		if err == MachineDone {
			return answer != nil
		}
		MaybePanic(err)
		if answer != nil {
			return true
		}
	}
}

func (self *machine) ProveAllParallel(goal interface{}) []Bindings {
	//we fire off a single thread and use a waitgroup until we all have returned
	return self.proveParallelThread(goal)
}

func (self *machine) proveParallelThread(goal interface{}) []Bindings {
	//var answer Bindings
	var answerlist []Bindings
	var err error
	answers := make([]Bindings, 0)

	goalTerm := self.toGoal(goal)
	vars := Variables(goalTerm) // preserve incoming human-readable names
	m := self.PushConj(goalTerm)
	for {
		m, answerlist, err = m.StepParallel()
		if answerlist != nil {
			for _, answer := range answerlist {
				if answer != nil {
					answers = append(answers, answer.WithNames(vars))
				}
			}
		}
		if err == MachineDone {
			break
		}
		MaybePanic(err)
	}

	return answers
}

// advance the Golog machine one step closer to proving the goal at hand.
// at the end of each invocation, the top item on the conjunctions stack
// is the goal we should next try to prove.
func (self *machine) StepParallel() (Machine, []Bindings, error) {
	var m Machine = self
	var goal Callable
	var err error
	var cp ChoicePoint

	//Debugf("stepping...\n%s\n", self)
	if false { // for debugging. commenting out needs import changes
		_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	}

	// find a goal other than true/0 to prove
	arity := 0
	functor := "true"
	for arity == 0 && functor == "true" {
		var mTmp Machine
		goal, mTmp, err = m.PopConj()
		if err == EmptyConjunctions { // found an answer
			answer := m.Bindings()
			Debugf("  emitting answer %s\n", answer)
			m = m.PushConj(NewAtom("fail")) // backtrack on next Step()
			return m, []Bindings{answer}, nil
		}
		MaybePanic(err)
		m = mTmp
		arity = goal.Arity()
		functor = goal.Name()
	}
	// are we proving a foreign predicate?
	f, ok := m.(*machine).lookupForeign(goal)
	if ok { // foreign predicate
		args := m.(*machine).resolveAllArguments(goal)
		Debugf("  running foreign predicate %s with %s\n", goal, args)
		ret := f(m, args)
		switch x := ret.(type) {
		case *foreignTrue:
			return m, nil, nil
		case *foreignFail:
			// do nothing. continue to iterate disjunctions below
		case *machine:
			return x, nil, nil
		case *foreignUnify:
			terms := []Term(*x) // guaranteed even number of elements
			env := m.Bindings()
			for i := 0; i < len(terms); i += 2 {
				env, err = terms[i].Unify(env, terms[i+1])
				if err == CantUnify {
					env = nil
					break
				}
				MaybePanic(err)
			}
			if env != nil {
				return m.SetBindings(env), nil, nil
			}
		}
	} else { // user-defined predicate, push all its disjunctions
		goal = goal.ReplaceVariables(m.Bindings()).(Callable)
		Debugf("  running user-defined predicate %s\n", goal)
		clauses, err := m.(*machine).db.Candidates(goal)
		MaybePanic(err)
		m = m.DemandCutBarrier()
		for i := len(clauses) - 1; i >= 0; i-- {
			clause := clauses[i]
			cp := NewHeadBodyChoicePoint(m, goal, clause)
			m = m.PushDisj(cp)
		}
	}

	// iterate disjunctions looking for one that succeeds
	var branchWait sync.WaitGroup
	//Needs a different method to avoid deadlock
	branchResultChan := make(chan []Bindings, 8192)
	//Need to clear all disjunctions in new choicepoint before following
	counter := 0
	for {
		m.DEBUG()
		cp, m, err = m.PopDisj()
		if err == EmptyDisjunctions { // nothing left to do
			break
		}
		MaybePanic(err)
		branchWait.Add(1)
		counter++

		go func(choice ChoicePoint) {
			defer branchWait.Done()
			// follow the next choice point
			mTmp, err := choice.Follow()
			switch err {
			case nil:
				_, answer, _ := mTmp.StepParallel()
				branchResultChan <- answer
				return
			case CutBarrierFails:
				Debugf("  ... skipping over cut barrier\n")
				return
			}
			MaybePanic(err)
			return
		}(cp)

	}
	branchWait.Wait()
	//drain the branchan, combine the answers and return
	retanswers := make([]Bindings, 0, 0)
	answermap := make(map[string]struct{})
	for len(branchResultChan) > 0 {
		answers := <-branchResultChan
		if len(answers) > 0 {
			for _, answer := range answers {
				pk := answer.PairingKey()
				if _, found := answermap[pk]; !found {
					answermap[pk] = struct{}{}
					retanswers = append(retanswers, answer)
				}
			}
		}
	}
	if len(retanswers) == 0 {
		return nil, nil, MachineDone
	}
	return nil, retanswers, MachineDone
}

func (self *machine) DEBUG() {
}
