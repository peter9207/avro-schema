package decoder

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/actgardner/gogen-avro/v7/generic"
	"github.com/landoop/schema-registry"
	"github.com/rs/zerolog/log"
)

type Decoder struct {
	client *schemaregistry.Client
}

func New(url string) (decoder Decoder, err error) {
	decoder.client, err = schemaregistry.NewClient(url)
	return
}

func (decoder Decoder) Decode(data []byte, event interface{}) (err error) {

	reader := bytes.NewReader(data)
	_, err = reader.ReadByte()
	if err != nil {
		log.Error().Msgf("error reading kafka message magic byte %v", err)
		return
	}

	var schemaID uint32
	err = binary.Read(reader, binary.BigEndian, &schemaID)
	if err != nil {
		log.Error().Msgf("error reading kafka message schemaID number %v", err)
		return
	}

	codec, err := getSchemaCodec(decoder.client, int(schemaID))
	if err != nil {
		log.Error().Msgf("failed to get schema codec %v", err)
		return
	}

	eventMap, err := codec.Deserialize(reader)
	if err != nil {
		log.Error().Msgf("failed to decode event %v", err)
		return
	}

	data, err = json.Marshal(eventMap)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &event)
	return
}

func getSchemaCodec(registry *schemaregistry.Client, schemaID int) (codec *generic.Codec, err error) {

	codec, ok := schemaCodecMapping[schemaID]
	if ok {
		return
	}

	log.Info().Int("schemaID", schemaID).Msg("did not find event in cache, attempting fetch from registry")

	schema, err := registry.GetSchemaByID(schemaID)
	if err != nil {
		return
	}
	codec, err = generic.NewCodecFromSchema([]byte(schema), []byte(schema))
	if err != nil {
		return
	}

	schemaCodecMapping[schemaID] = codec
	return
}

var schemaCodecMapping = map[int]*generic.Codec{}
