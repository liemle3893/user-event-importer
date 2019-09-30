package bitmap

import "errors"

type ItemCallback func(string)

var (
	UnsupportedTypeError = errors.New("Unsupported id type")
)

type BitMap interface {
	Key() string
	Description() string
	// Set description
	Desc(desc string)

	Count() (uint64, error)
	Elements(cb ItemCallback) error
	// ListElements list all elements inside bitmap. This can be very slow.
	ListElements() ([]string, error)
	// Exists check if id is exists in bitmap or not.
	// May return UnsupportedTypeError type is not supported
	Exists(id string) (bool, error)

	// Or Return new BitMap
	Or(b0 BitMap, bs ...BitMap) (BitMap, error)
	// And Return new BitMap
	And(b0 BitMap, bs ...BitMap) (BitMap, error)
	// Xor Return new BitMap
	Xor(b0 BitMap, bs ...BitMap) (BitMap, error)
	// Not Return new BitMap
	Not() (BitMap, error)

	// Mark id in Bitmap
	// May return UnsupportedTypeError type is not supported
	Mark(id string) error
	// Convenience method for Mark
	MarkInt(id int) error

	ToBytes() ([]byte, error)
	FromBytes(bytes []byte) error
}
