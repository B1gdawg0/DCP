package state

import p "github.com/B1gdawg0/DCP/proto"

func Transition(current State, msg p.MessageType) (State, error) {

	switch current {
		case StatePending:
			switch msg {
			case p.TypeRequest:
				return StatePending, nil // idempotent retry
			case p.TypeAccepted:
				return StateAccepted, nil
			case p.TypeRejected:
				return StateFailed, nil
			case p.TypeCancelled:
				return StateCancelled, nil
			case p.TypeExpired:
				return StateExpired, nil
			}
			
		case StateAccepted:
			switch msg {
			case p.TypeAccepted:
				return StateAccepted, nil // duplicate ACCEPT
			case p.TypeCompleted:
				return StateCompleted, nil
			case p.TypeFailed:
				return StateFailed, nil
			case p.TypeCancelled:
				return StateCancelled, nil
			case p.TypeExpired:
				return StateExpired, nil
			}


		case StateCompleted:
			switch msg {
			case p.TypeCompleted:
				return StateCompleted, nil // retry from receiver
			case p.TypeACK:
				return StateCompleted, nil // final confirmation
			}

		case StateFailed:
			switch msg {
			case p.TypeFailed:
				return StateFailed, nil
			case p.TypeACK:
				return StateFailed, nil
			}

		case StateCancelled:
			switch msg {
			case p.TypeCancelled:
				return StateCancelled, nil
			case p.TypeACK:
				return StateCancelled, nil
			}

		case StateExpired:
			switch msg {
			case p.TypeExpired:
				return StateExpired, nil
			case p.TypeACK:
				return StateExpired, nil
			}
	}

	return 0, ErrInvalidTransition
}