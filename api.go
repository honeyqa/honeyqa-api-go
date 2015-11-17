package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
	"github.com/streadway/amqp"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

// RabbitMQ
type rabbit_session struct {
	conn       *amqp.Connection
	ch         *amqp.Channel
	android_q  amqp.Queue
	android_nq amqp.Queue
	ios_q      amqp.Queue
}

func connectRabbit() (s rabbit_session) {
	conn, err := amqp.Dial("amqp://id:pw@host:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	aq, err := ch.QueueDeclare(
		"oqa_android_log_queue", // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")
	anq, err := ch.QueueDeclare(
		"oqa_android_native_log_queue", // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")
	iq, err := ch.QueueDeclare(
		"oqa_ios_log_queue", // name
		true,                // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare a queue")
	return rabbit_session{conn, ch, aq, anq, iq}
}

// Redis Pool
// https://github.com/garyburd/redigo/blob/master/redis/pool.go#L51
func connectRedis(host, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func SessionCount(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	data, err := parseJson(r.Body)
	if err != nil {
		w.WriteHeader(400)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"msg\":\"not valid json\"}"))
	} else {
		redis_conn := redisPool.Get()
		defer redis_conn.Close() // Connection must be closed after using
		var m int = time.Now().Minute()
		r, err := redis.Int(redis_conn.Do("HGET", "hqa_projects", data["apikey"].(string)))
		if err != nil {
			w.WriteHeader(500)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("{\"msg\":\"REDIS_ERR\"}"))
		} else {
			if r == 0 {
				w.WriteHeader(400)
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte("{\"msg\":\"APIKEY NOT EXISTS\"}"))
			} else {
				r2, err := redis_conn.Do("HINCRBY", "hqa_session_"+strconv.Itoa(m/15), r, 1)
				if err != nil {
					w.WriteHeader(500)
					w.Header().Set("Content-Type", "application/json")
					w.Write([]byte("{\"msg\":\"REDIS_ERR\"}"))
				} else {
					if r2 != 0 {
						w.WriteHeader(200)
						w.Header().Set("Content-Type", "application/json")
						w.Write([]byte("{\"msg\":\"success\"}"))
					} else {
						w.WriteHeader(500)
						w.Header().Set("Content-Type", "application/json")
						w.Write([]byte("{\"msg\":\"REDIS_ERR\"}"))
					}
				}
			}
		}
	}
}

func InsertAndroidLog(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	defer r.Body.Close()
	data, err := parseJson(r.Body)
	if err != nil {
		w.WriteHeader(400)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"msg\":\"not valid json\"}"))
	} else {
		resp, _ := json.Marshal(data)
		rabbit.ch.Publish("", rabbit.android_q.Name, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        resp,
		})
		w.WriteHeader(200)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"msg\":\"success\"}"))
	}
}

func InsertAndroidNativeLog(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	defer r.Body.Close()
	data, err := parseJson(r.Body)
	if err != nil {
		w.WriteHeader(400)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"msg\":\"not valid json\"}"))
	} else {
		resp, _ := json.Marshal(data)
		rabbit.ch.Publish("", rabbit.android_nq.Name, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        resp,
		})
		w.WriteHeader(200)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"msg\":\"success\"}"))
	}
}

func parseJson(b io.Reader) (d map[string]interface{}, err error) {
	decoder := json.NewDecoder(b)
	var j map[string]interface{}
	e := decoder.Decode(&j)
	if e != nil {
		return nil, e
	}
	return j, nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

var rabbit = connectRabbit()
var redisPool = connectRedis(":6379", "")

func main() {
	router := httprouter.New()
	// iOS
	// Session
	router.POST("/api/ios/client/session", SessionCount)
	// Android
	// Session
	router.POST("/api/v2/client/session", SessionCount)
	// Exception
	router.POST("/api/v2/client/exception", InsertAndroidLog)
	router.POST("/api/v2/client/exception/native", InsertAndroidNativeLog)
	// HTTPS
	log.Fatal(http.ListenAndServeTLS(":443", "crt.crt", "key.key", router))
}
