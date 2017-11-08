package lib

type Queue struct {
	queue []interface{}
	size  int
	head  int
	tail  int
}

func NewQueue() *Queue {
	return &Queue{
		queue: make([]interface{}, 100),
		size:  0,
		head:  0,
		tail:  0,
	}
}

func (q *Queue) Size() int {
	return q.size
}

func (q *Queue) Push(e interface{}) {
	if q.head == q.tail && q.size > 0 {
		queue := make([]interface{}, len(q.queue)*2)
		copy(queue, q.queue[q.head:])
		copy(queue[len(q.queue)-q.head:], q.queue[:q.head])
		q.head = 0
		q.tail = len(q.queue)
		q.queue = queue
	}
	q.queue[q.tail] = e
	q.tail = (q.tail + 1) % len(q.queue)
	q.size++
}

func (q *Queue) Pop() interface{} {
	if q.size == 0 {
		return nil
	}
	e := q.queue[q.head]
	q.head = (q.head + 1) % len(q.queue)
	q.size--
	return e
}
