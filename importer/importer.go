package importer

import (
	"github.com/hashicorp/go-multierror"
	"github.com/liemle3893/user-event-importer/event"
)

type ImportFunc func(e ...event.Event) (int32, error)

type Importer interface {
	// Import events and return number of imported events
	Import(events ...event.Event) (int32, error)
}

func NewImporter(fn ImportFunc) Importer {
	return wrappedFunc{fn}
}

func NewCompositeImporter(importers ...Importer) *CompositeImporter {
	return &CompositeImporter{importers: importers}
}

type CompositeImporter struct {
	importers []Importer
}

func (c *CompositeImporter) AppendImporter(importer Importer) {
	c.importers = append(c.importers, importer)
}

func (c *CompositeImporter) AppendImportFunc(fn ImportFunc) {
	c.AppendImporter(wrappedFunc{fn})
}

type wrappedFunc struct {
	fn ImportFunc
}

func (i wrappedFunc) Import(e ...event.Event) (int32, error) {
	return i.fn(e...)
}

type wrappedImporter struct {
	Importer
}

func (i wrappedImporter) import0(ch chan importResult, e []event.Event) {
	go func() {
		count, err := i.Importer.Import(e...)
		ch <- importResult{count, err}
	}()
}

func (c *CompositeImporter) Import(e ...event.Event) (int32, error) {
	var importedEvents int32
	var err *multierror.Error
	resultCh := make(chan importResult)
	for _, importer := range c.importers {
		wImporter := wrappedImporter{importer}
		wImporter.import0(resultCh, e)
	}
	for i := 0; i < len(c.importers); i++ {
		result := <-resultCh
		if result.err != nil {
			err = multierror.Append(err, result.err)
		} else {
			importedEvents += result.count
		}
	}
	close(resultCh)
	return importedEvents, err.ErrorOrNil()
}

type importResult struct {
	count int32
	err   error
}
