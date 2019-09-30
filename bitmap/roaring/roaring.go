package roaring

import (
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/liemle3893/user-event-importer/bitmap"
	"strconv"
	"strings"
)

type roaringBitMap struct {
	internal *roaring.Bitmap
	key      string
	desc     string
}

func (r *roaringBitMap) ListElements() ([]string, error) {
	var elements []string
	for it := r.internal.Iterator(); it.HasNext(); {
		elements = append(elements, fmt.Sprint(it.Next()))
	}
	return elements, nil
}

func (r *roaringBitMap) FromBytes(bytes []byte) error {
	internal := roaring.New()
	_, err := internal.FromBuffer(bytes)
	r.internal = internal
	return err
}

func (r *roaringBitMap) ToBytes() ([]byte, error) {
	return r.internal.ToBytes()
}

func (r *roaringBitMap) MarkInt(idx int) error {
	return r.Mark(fmt.Sprint(idx))
}

func (r *roaringBitMap) Key() string {
	return r.key
}

func (r *roaringBitMap) Description() string {
	return r.desc
}

func (r *roaringBitMap) Count() (uint64, error) {
	return r.internal.GetCardinality(), nil
}

func (r *roaringBitMap) Elements(cb bitmap.ItemCallback) error {
	for it := r.internal.Iterator(); it.HasNext(); {
		cb(fmt.Sprint(it.Next()))
	}
	return nil
}

func (r *roaringBitMap) Exists(id string) (bool, error) {
	if idx, err := strconv.ParseInt(id, 10, 64); err != nil {
		return false, bitmap.UnsupportedTypeError
	} else {
		return r.internal.Contains(uint32(idx)), nil
	}
}

func (r *roaringBitMap) Or(b0 bitmap.BitMap, bs ...bitmap.BitMap) (bitmap.BitMap, error) {
	bitmaps := r.cloneBitmaps(b0, bs...)
	bmInternal := roaring.New()
	var keys []string
	for _, b := range bitmaps {
		bm := createRoaringBitmapIfNeeded(b)
		bmInternal.Or(bm.internal)
		keys = append(keys, b.Key())
	}
	return &roaringBitMap{
		internal: bmInternal,
		key:      strings.Join(keys, "___OR___"),
		desc:     r.desc,
	}, nil
}

func (r *roaringBitMap) And(b0 bitmap.BitMap, bs ...bitmap.BitMap) (bitmap.BitMap, error) {
	bitmaps := r.cloneBitmaps(b0, bs...)
	// Must use r.internal because of AND
	bmInternal := r.internal.Clone()
	var keys []string
	for _, b := range bitmaps {
		bm := createRoaringBitmapIfNeeded(b)
		bmInternal.And(bm.internal)
		keys = append(keys, b.Key())
	}

	return &roaringBitMap{
		internal: bmInternal,
		key:      strings.Join(keys, "___AND___"),
		desc:     r.desc,
	}, nil
}

func (r *roaringBitMap) Xor(b0 bitmap.BitMap, bs ...bitmap.BitMap) (bitmap.BitMap, error) {
	bitmaps := r.cloneBitmaps(b0, bs...)
	bmInternal := roaring.New()
	var keys []string
	for _, b := range bitmaps {
		bm := createRoaringBitmapIfNeeded(b)
		bmInternal.Xor(bm.internal)
		keys = append(keys, b.Key())
	}

	return &roaringBitMap{
		internal: bmInternal,
		key:      strings.Join(keys, "___XOR___"),
		desc:     r.desc,
	}, nil
}

func (r *roaringBitMap) Not() (bitmap.BitMap, error) {
	var upperBound uint32
	if r.internal.IsEmpty() {
		upperBound = 0
	} else {
		upperBound = r.internal.Maximum()
	}
	bmInternal := r.internal.Clone()
	bmInternal.Flip(0, uint64(upperBound))
	var key string
	if strings.HasPrefix(r.Key(), "___NOT___") {
		key = r.Key()[9:]
	} else {
		key = "___NOT___" + r.Key()
	}
	return &roaringBitMap{
		internal: bmInternal,
		key:      key,
		desc:     r.desc,
	}, nil
}

func (r *roaringBitMap) Mark(id string) (error) {
	if idx, err := strconv.ParseUint(id, 10, 64); err != nil {
		return bitmap.UnsupportedTypeError
	} else {
		r.internal.Add(uint32(idx))
		return nil
	}
}

func (r *roaringBitMap) Desc(desc string) {
	r.desc = desc
}

func createRoaringBitmapIfNeeded(b0 bitmap.BitMap) *roaringBitMap {
	var b, ok = b0.(*roaringBitMap)
	if ok {
		return b
	}
	b = &roaringBitMap{
		internal: roaring.NewBitmap(),
		key:      b.Key(),
		desc:     b.Description(),
	}
	var data []uint32
	b.Elements(func(idx string) {
		if id, err := strconv.ParseUint(idx, 10, 64); err != nil {
			panic(fmt.Sprintf("%s is not an int", idx))
		} else {
			data = append(data, uint32(id))
		}
	})
	b.internal.AddMany(data)
	return b
}

func NewBitMap(key, desc string) bitmap.BitMap {
	return &roaringBitMap{
		internal: roaring.New(),
		key:      key,
		desc:     desc,
	}
}

func FromBytes(bytes []byte, key, desc string) (bitmap.BitMap, error) {
	bm := NewBitMap(key, desc)
	err := bm.FromBytes(bytes)
	return bm, err
}

func (r *roaringBitMap) cloneBitmaps(b0 bitmap.BitMap, bs ...bitmap.BitMap) []bitmap.BitMap {
	var bitmaps []bitmap.BitMap
	// Copy bitmaps
	bitmaps = append(bitmaps, r, b0)
	if len(bs) > 0 {
		bitmaps = append(bitmaps, bs...)
	}
	return bitmaps
}
