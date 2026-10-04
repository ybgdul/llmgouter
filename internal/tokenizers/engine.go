package tokenizers

import (
	"fmt"

	"github.com/daulet/tokenizers"
)

type TokenizerEngine struct{ 
	Tokenizer *tokenizers.Tokenizer
}

func NewEngine() (*TokenizerEngine, error) { 
	tk, err := tokenizers.FromPretrained("google-bert/bert-base-uncased")
	if err != nil { 
		return nil, fmt.Errorf("tokenizer initialization failed: %w", err)
	}
	return &TokenizerEngine{
		Tokenizer: tk,
	}, nil
}

func (t *TokenizerEngine) tokenize() (int, error) { 
	return 0, fmt.Errorf("unimplemented tokenize method")
}



