package langchain

import (
	"errors"

	"github.com/tmc/langchaingo/textsplitter"
	"golang.org/x/sync/semaphore"
)

const (
	SemaphoreAcquireError = "failed to acquire langchain semaphore"
)

type Client struct {
	ClientConfig *ClientConfig `json:"client_config"`
}

type ClientConfig struct {
	MaxConcurrentRequests int64
	sem                   *semaphore.Weighted
}

func NewClient(config ClientConfig) *Client {
	config.sem = semaphore.NewWeighted(config.MaxConcurrentRequests)
	return &Client{
		ClientConfig: &config,
	}
}

func (c *Client) SplitTextViaMarkdownTextSplitter(text string, options *textsplitter.Options) ([]string, error) {
	// we are using TryAcquire to avoid blocking the main thread
	acquired := c.ClientConfig.sem.TryAcquire(1)
	if !acquired {
		return nil, errors.New(SemaphoreAcquireError)
	}

	defer c.ClientConfig.sem.Release(1)

	optionList := convertOptionsToOptionList(options)

	textSplitter := textsplitter.NewMarkdownTextSplitter(optionList...)
	return textSplitter.SplitText(text)
}

func (c *Client) SplitTextViaRecursiveCharacterTextSplitter(
	text string, options *textsplitter.Options,
) ([]string, error) {
	// we are using TryAcquire to avoid blocking the main thread
	acquired := c.ClientConfig.sem.TryAcquire(1)
	if !acquired {
		return nil, errors.New(SemaphoreAcquireError)
	}

	defer c.ClientConfig.sem.Release(1)

	optionList := convertOptionsToOptionList(options)

	textSplitter := textsplitter.NewRecursiveCharacter(optionList...)
	return textSplitter.SplitText(text)
}

func (c *Client) SplitTextViaTokenTextSplitter(text string, options *textsplitter.Options) ([]string, error) {
	// we are using TryAcquire to avoid blocking the main thread
	acquired := c.ClientConfig.sem.TryAcquire(1)
	if !acquired {
		return nil, errors.New(SemaphoreAcquireError)
	}

	defer c.ClientConfig.sem.Release(1)

	optionList := convertOptionsToOptionList(options)

	textSplitter := textsplitter.NewTokenSplitter(optionList...)
	return textSplitter.SplitText(text)
}

func convertOptionsToOptionList(options *textsplitter.Options) []textsplitter.Option {
	optionList := []textsplitter.Option{}
	if options.ChunkSize > 0 {
		optionList = append(optionList, textsplitter.WithChunkSize(options.ChunkSize))
	}
	if options.ChunkOverlap > 0 {
		optionList = append(optionList, textsplitter.WithChunkOverlap(options.ChunkOverlap))
	}
	if len(options.Separators) > 0 {
		optionList = append(optionList, textsplitter.WithSeparators(options.Separators))
	}
	if options.KeepSeparator {
		optionList = append(optionList, textsplitter.WithKeepSeparator(options.KeepSeparator))
	}
	if options.LenFunc != nil {
		optionList = append(optionList, textsplitter.WithLenFunc(options.LenFunc))
	}
	if options.ModelName != "" {
		optionList = append(optionList, textsplitter.WithModelName(options.ModelName))
	}
	if options.EncodingName != "" {
		optionList = append(optionList, textsplitter.WithEncodingName(options.EncodingName))
	}
	if len(options.AllowedSpecial) > 0 {
		optionList = append(optionList, textsplitter.WithAllowedSpecial(options.AllowedSpecial))
	}
	if len(options.DisallowedSpecial) > 0 {
		optionList = append(optionList, textsplitter.WithDisallowedSpecial(options.DisallowedSpecial))
	}
	if options.SecondSplitter != nil {
		optionList = append(optionList, textsplitter.WithSecondSplitter(options.SecondSplitter))
	}
	if len(options.DisallowedSpecial) > 0 {
		optionList = append(optionList, textsplitter.WithDisallowedSpecial(options.DisallowedSpecial))
	}
	if options.SecondSplitter != nil {
		optionList = append(optionList, textsplitter.WithSecondSplitter(options.SecondSplitter))
	}
	if options.CodeBlocks {
		optionList = append(optionList, textsplitter.WithCodeBlocks(options.CodeBlocks))
	}
	if options.ReferenceLinks {
		optionList = append(optionList, textsplitter.WithReferenceLinks(options.ReferenceLinks))
	}
	if options.JoinTableRows {
		optionList = append(optionList, textsplitter.WithJoinTableRows(options.JoinTableRows))
	}
	if options.KeepHeadingHierarchy {
		optionList = append(optionList, textsplitter.WithHeadingHierarchy(options.KeepHeadingHierarchy))
	}
	return optionList
}
