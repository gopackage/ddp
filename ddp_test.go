package ddp_test

import (
	"encoding/json"

	. "github.com/gopackage/ddp"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Core", func() {

	Describe("Doc", func() {

		It("should navigate a 1 deep path", func() {
			var data interface{}
			err := json.Unmarshal([]byte(`{"foo":"bar"}`), &data)
			Ω(err).ShouldNot(HaveOccurred())
			doc := NewDoc(data)
			item := doc.Item("foo")
			Ω(item).ShouldNot(BeNil())
			value := doc.String("foo")
			Ω(value).ShouldNot(BeNil())
			Ω(value).Should(Equal("bar"))
		})

		It("should navigate a 2 deep path", func() {
			var data interface{}
			err := json.Unmarshal([]byte(`{"hello":{"foo":"bar"}}`), &data)
			Ω(err).ShouldNot(HaveOccurred())
			doc := NewDoc(data)
			item := doc.Item("hello.foo")
			Ω(item).ShouldNot(BeNil())
			value := doc.String("hello.foo")
			Ω(value).ShouldNot(BeNil())
			Ω(value).Should(Equal("bar"))
		})

		It("should set a 2 deep path", func() {
			var data interface{}
			err := json.Unmarshal([]byte(`{}`), &data)
			Ω(err).ShouldNot(HaveOccurred())
			doc := NewDoc(data)
			doc.Set("hello.foo", "bar")
			value := doc.String("hello.foo")
			Ω(value).ShouldNot(BeNil())
			Ω(value).Should(Equal("bar"))
			txt, err := json.Marshal(data)
			Ω(err).ShouldNot(HaveOccurred())
			Ω(string(txt)).Should(Equal(`{"hello":{"foo":"bar"}}`))
		})
	})
})
