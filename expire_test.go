package gostore_test

import (
	"log"
	"time"

	"github.com/tonjun/gostore"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Expire", func() {

	var store gostore.Store

	BeforeEach(func() {
		log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
		store = gostore.NewStore()
		store.Init()
	})

	AfterEach(func() {
		store.Close()
	})

	It("OnItemExpire callback should be called when an item expires", func() {

		i := &gostore.Item{}

		ch := make(chan gostore.Item)

		store.OnItemExpire(func(item *gostore.Item) {
			log.Printf("Item expired callback!: key: \"%s\" value: \"%s\"", item.Key, item.Value.(string))
			Expect(item.Key).To(Equal("k1"))
			Expect(item.Value).To(Equal("d1"))
			ch <- *item
		})

		err := store.Set(&gostore.Item{ID: "d1", Key: "k1", Value: "d1"}, 200*time.Millisecond)
		Expect(err).Should(BeNil())
		err = store.Set(&gostore.Item{ID: "d2", Key: "k2", Value: "d2"}, 20*time.Second)
		Expect(err).Should(BeNil())
		err = store.Set(&gostore.Item{ID: "d1", Key: "k3", Value: "d1"}, 20*time.Second)
		Expect(err).Should(BeNil())

		time.Sleep(1 * time.Second)

		Expect(ch).Should(Receive(i))
		Consistently(ch).ShouldNot(Receive())

		it, found, err := store.Get("k1")
		Expect(it).To(BeNil())
		Expect(found).To(BeFalse())
		Expect(err).To(BeNil())

		time.Sleep(1 * time.Second)

	})

	It("Should reset the expiry by setting a key to a new value", func() {

		ch := make(chan bool)

		store.OnItemExpire(func(item *gostore.Item) {
			ch <- true
		})

		store.Set(&gostore.Item{Key: "keyone", ID: "1", Value: "data1"}, 200*time.Millisecond)
		time.Sleep(50 * time.Millisecond)
		store.Set(&gostore.Item{Key: "keyone", ID: "2", Value: "data2"}, 5*time.Second)
		Consistently(ch, "1s").ShouldNot(Receive())

		store.Set(&gostore.Item{Key: "keyone", ID: "1", Value: "data1"}, 5*time.Second)
		Consistently(ch, "1s").ShouldNot(Receive())

		store.Set(&gostore.Item{Key: "keyone", ID: "1", Value: "data1"}, 200*time.Millisecond)
		Eventually(ch, "1s").Should(Receive())
		Consistently(ch, "1s").ShouldNot(Receive())
	})

})
