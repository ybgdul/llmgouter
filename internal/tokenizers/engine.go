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

func (t *TokenizerEngine) CountTokens(text string) int { 
	tokens, _ := t.Tokenizer.Encode(text, true)
	return len(tokens)
}

func (t *TokenizerEngine) ExtractPrefixHash(text string, numTokens int) string { 
	tokens, _ := t.Tokenizer.Encode(text, true)
	if len(tokens) < numTokens {
		return fmt.Sprintf("%v", tokens)
	}
	return fmt.Sprintf("%v", tokens[:numTokens])
}




