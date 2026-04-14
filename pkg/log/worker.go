package log

type worker struct {
	log *Log

	done chan struct{}
}

func (w *worker) run() {
	for data := range w.log.buf {
		if err := w.log.activeSegment.append(data); err != nil {
			w.error(data, err)
			continue
		}
		if w.log.activeSegment.isMaxed() {
			if err := w.log.newSegment(w.log.activeSegment.uid + 1); err != nil {
				w.error(nil, err)
			}
		}
	}
	w.done <- struct{}{}
}

func (w *worker) flush() {
	<-w.done
	for _, segment := range w.log.segments {
		if err := segment.flush(); err != nil {
			w.error(nil, err)
		}
	}
}

func (w *worker) error(data []byte, err error) {
	if w.log.errs != nil {
		w.log.errs <- &LogError{
			error: err,
			Data:  data,
		}
	}
}
