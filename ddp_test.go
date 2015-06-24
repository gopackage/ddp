package ddp_test

import (
	"encoding/json"

	. "github.com/badslug/ddp"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Core", func() {

	Describe("Doc", func() {

		It("should split paths", func() {
			var data interface{}
			err := json.Unmarshal([]byte(`{"foo":"bar"}`), &data)
			Ω(err).ShouldNot(HaveOccurred())
			doc := NewDoc(data)
			dir, key, err := doc.Split([]string{"foo"})
			Ω(err).ShouldNot(HaveOccurred())
			Ω(dir).Should(Equal([]string{}))
			Ω(key).Should(Equal("foo"))
			_, _, err = doc.Split([]string{})
			Ω(err).Should(HaveOccurred())
		})

		It("should navigate a 1 deep path", func() {
			var data interface{}
			err := json.Unmarshal([]byte(`{"foo":"bar"}`), &data)
			Ω(err).ShouldNot(HaveOccurred())
			doc := NewDoc(data)
			item, err := doc.ItemForPath([]string{"foo"})
			Ω(err).ShouldNot(HaveOccurred())
			Ω(item).ShouldNot(BeNil())
			value, err := doc.GetStringForPath([]string{"foo"})
			Ω(err).ShouldNot(HaveOccurred())
			Ω(value).Should(Equal("bar"))
		})

		It("should navigate a 2 deep path", func() {
			var data interface{}
			err := json.Unmarshal([]byte(`{"hello":{"foo":"bar"}}`), &data)
			Ω(err).ShouldNot(HaveOccurred())
			doc := NewDoc(data)
			item, err := doc.ItemForPath([]string{"hello", "foo"})
			Ω(err).ShouldNot(HaveOccurred())
			Ω(item).ShouldNot(BeNil())
			value, err := doc.GetStringForPath([]string{"hello", "foo"})
			Ω(err).ShouldNot(HaveOccurred())
			Ω(value).Should(Equal("bar"))
		})
	})
})
