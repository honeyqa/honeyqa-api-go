package main

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
	"net/http"
)

type rabbit_session struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	q    amqp.Queue
}

func connectRabbit() (s rabbit_session) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	q, err := ch.QueueDeclare(
		"oqa_log_queue", // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a queue")
	return rabbit_session{conn, ch, q}
}

func InsertLog(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	decoder := json.NewDecoder(r.Body)
	var j map[string]interface{}
	err := decoder.Decode(&j)
	if err != nil {
		w.WriteHeader(400)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"msg\":\"not valid json\"}"))
	} else {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("{\"msg\":\"failed to read data\"}"))
		} else {
			err = rabbit.ch.Publish(
				"",            // exchange
				rabbit.q.Name, // routing key
				false,         // mandatory
				false,         // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        body,
				},
			)
			if err != nil {
				w.WriteHeader(500)
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte("{\"msg\":\"failed to insert data\"}"))
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte("{\"msg\":\"data inserted\"}"))
			}
		}
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

var rabbit = connectRabbit()

func main() {
	router := httprouter.New()
	router.POST("/crash", InsertLog)
	log.Fatal(http.ListenAndServe(":8080", router))
}
