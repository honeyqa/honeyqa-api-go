package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
	"github.com/streadway/amqp"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

// RabbitMQ
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
						w.Write([]byte("{\"msg\":\"parsed\"}"))
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

func InsertLog(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	// TODO : JSON valid check then pass data to Queue
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
	// router.POST("/api/ios/client/exception", InsertLog)
	log.Fatal(http.ListenAndServe(":8080", router))
}
