package server

import (
	"math/rand"

	"github.com/redis-geo/redgeo/parser"
	"github.com/redis-geo/redgeo/redisapi"
)

var lolwutAnswers = []string{
	// yes
	"As I see it, yes",
	"It is certain",
	"It is decidedly so",
	"Most likely",
	"Outlook good",
	"Signs point to yes",
	"Without a doubt",
	"Yes definitely",
	"Yes",
	"You may rely on it",
	// maybe
	"Ask again later",
	"Better not tell you now",
	"Cannot predict now",
	"Concentrate and ask again",
	"Reply hazy, try again",
	// no
	"Don't count on it",
	"My reply is no",
	"My sources say no",
	"Outlook not so good",
	"Very doubtful",
}

// Answers any question you throw at it
// with magic ⋆｡𖦹°⭒˚｡⋆
// LOLWUT [question...]
type Lolwut struct {
	redis.BaseCmd
	parts []string
}

func ParseLolwut(b redis.BaseCmd) (Lolwut, error) {
	cmd := Lolwut{BaseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.parts),
	).Required(0).Run(cmd.Args())
	if err != nil {
		return Lolwut{}, err
	}
	return cmd, nil
}

func (c Lolwut) Run(w redis.Writer, _ redis.Redka) (any, error) {
	var answer string
	if len(c.parts) != 0 {
		answer = lolwutAnswers[rand.Intn(len(lolwutAnswers))]
	} else {
		answer = "Ask me a question (⊃｡•́‿•̀｡)⊃"
	}
	w.WriteBulkString(answer + "\n")
	return answer, nil
}
