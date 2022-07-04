package TokenBucket

import "time"

type TokenBucket struct {
	maxTokens       int
	remainingTokens int
	prevReset       int64
	resetInterval   int64
}

func GenerateTokenBuket(numTokens int, resetInter int) *TokenBucket {
	tokenBucket := TokenBucket{}
	tokenBucket.maxTokens = numTokens
	tokenBucket.remainingTokens = numTokens
	tokenBucket.prevReset = time.Now().Unix()
	tokenBucket.resetInterval = int64(resetInter)
	return &tokenBucket
}

func (tokenBucket *TokenBucket) Update() bool {
	timeNow := time.Now().Unix()
	if timeNow-tokenBucket.prevReset > tokenBucket.resetInterval {
		tokenBucket.remainingTokens = tokenBucket.maxTokens
		tokenBucket.prevReset = timeNow
	}
	if tokenBucket.remainingTokens <= 0 {
		return false
	}
	tokenBucket.remainingTokens--
	return true
}
