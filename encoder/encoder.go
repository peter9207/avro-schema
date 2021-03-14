package encoder

import (
	// "bytes"
	"encoding/binary"
	// "encoding/json"
	// "github.com/actgardner/gogen-avro/v7/generic"
	"github.com/landoop/schema-registry"
	// "github.com/rs/zerolog/log"

	"fmt"
	"github.com/linkedin/goavro/v2"
)

type Encoder struct {
	client   *schemaregistry.Client
	schema   string
	subject  string
	codec    *goavro.Codec
	schemaID int
}

func New(url, schema, subject string) (encoder Encoder, err error) {
	encoder.schema = schema
	encoder.subject = subject
	encoder.client, err = schemaregistry.NewClient(url)
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return
	}

	encoder.codec = codec
	schemaID, err := encoder.client.RegisterNewSchema(encoder.subject, schema)
	if err != nil {
		return
	}
	encoder.schemaID = schemaID
	return
}

func (encoder Encoder) Encode(event interface{}) (message []byte, err error) {

	data, err := encoder.codec.BinaryFromNative(nil, event)
	if err != nil {
		return
	}

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(encoder.schemaID))
	message = append(message, []byte{0}...)
	message = append(message, schemaIDBytes...)
	message = append(message, data...)

	return
}
