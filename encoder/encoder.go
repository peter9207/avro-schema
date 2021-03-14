package encoder

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/actgardner/gogen-avro/v7/generic"
	"github.com/landoop/schema-registry"
	"github.com/rs/zerolog/log"

	"github.com/linkedin/goavro/v2"
)

type Encoder struct {
	client  *schemaregistry.Client
	schema  string
	subject string
}

func New(url, schema, subject string) (encoder Encoder, err error) {
	encoder.schema = schema
	encoder.subject = subject
	encoder.client, err = schemaregistry.NewClient(url)
	return
}

func (encoder Encoder) Encoder(subject, schema string, event interface{}) (message []byte, err error) {
	schemaID, err := encoder.client.RegisterEvent(encoder.subject, encoder.subject)
	if err != nil {
		panic(err)
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return
	}

	data, err := codec.BinaryFromNative(nil, event)
	if err != nil {
		return
	}

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID))
	message = append(message, []byte{0}...)
	message = append(message, schemaIDBytes...)
	message = append(message, data...)

	return
}
