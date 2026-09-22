package mr

// Bitmap is a simple bit-set backed by a byte slice.
type Bitmap []byte

// NewBitmap creates a bitmap large enough to hold `n` elements.
func NewBitmap(n uint32) Bitmap {
	return make(Bitmap, (n+7)/8) // round up to nearest byte
}

// Set sets bit i to 1.
func (b Bitmap) Set(i uint32) {
	b[i/8] |= 1 << (i % 8)
}

// Clear sets bit i to 0.
func (b Bitmap) Clear(i uint32) {
	b[i/8] &^= 1 << (i % 8)
}

// Get returns true if bit i is set.
func (b Bitmap) Get(i uint32) bool {
	return b[i/8]&(1<<(i%8)) != 0
}

// Toggle flips bit i.
func (b Bitmap) Toggle(i uint32) {
	b[i/8] ^= 1 << (i % 8)
}

// Len returns the number of bits the bitmap can hold.
func (b Bitmap) Len() uint32 {
	return uint32(len(b)) * 8
}

func (b Bitmap) Add(s Bitmap) {
	for i := range len(b) {
		b[i] = b[i] | s[i]
	}
}

// Count returns the number of set bits (popcount).
func (b Bitmap) Count() uint32 {
	var count uint32 = 0
	for _, byteVal := range b {
		count += popcount(byteVal)
	}
	return count
}

func popcount(x byte) uint32 {
	var count uint32 = 0
	for x != 0 {
		count += uint32(x & 1)
		x >>= 1
	}
	return count
}

// Reset clears all bits.
func (b Bitmap) Reset() {
	for i := range b {
		b[i] = 0
	}
}
