package ipfsblob

type Blob interface {
	Put([]byte) ([]byte, error) // Store blob and return identifier
	Get([]byte) ([]byte, error) // Get blob by identifier
	Del([]byte) error           // Attempt to delete object

	// Only used for tests?
	//
	// Enum(func([]byte, []byte) error) error // Enumerate over all objects
}
