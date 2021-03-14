package avro_schema_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"encoding/json"
	"github.com/peter9207/avro-schema/decoder"
	"github.com/peter9207/avro-schema/encoder"
)

func TestAvroSchema(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AvroSchema Suite")
}

type TestEvent struct {
	StringField string `json:"stringField"`
	IntField    int    `json:"intField"`
}

var schemaString = `
{
  "name": "MyClass",
  "type": "record",
  "namespace": "com.acme.avro",
  "fields": [
    {
      "name": "stringField",
      "type": "string"
    },
    {
      "name": "intField",
      "type": "int"
    }
  ]
}
`

var _ = Describe("Encoding and decoding", func() {
	registryURL := "http://localhost:8081"

	It("should encode and decode the same event", func() {

		encoder, err := encoder.New(registryURL, schemaString, "testsubject")
		Ω(err).Should(BeNil())

		decoder, err := decoder.New(registryURL)
		Ω(err).Should(BeNil())

		event1 := TestEvent{
			StringField: "teststring",
			IntField:    1,
		}

		jsonData, err := json.Marshal(event1)
		Ω(err).Should(BeNil())

		dataMap := map[string]interface{}{}

		err = json.Unmarshal(jsonData, &dataMap)
		Ω(err).Should(BeNil())

		message, err := encoder.Encode(dataMap)
		Ω(err).Should(BeNil())

		resultEvent := TestEvent{}

		err = decoder.Decode(message, &resultEvent)
		Ω(err).Should(BeNil())

		Ω(resultEvent).Should(Equal(event1))

	})

})
