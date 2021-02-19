package template_miner

import (
	"encoding/json"
	"regexp"
)

type MaskingInstruction struct {
	RegexPattern string `json:"regex_pattern"`
	MaskWith     string `json:"mask_with"`

	Regex *regexp.Regexp
}

type RegexMasker struct {
	maskingInstructions []MaskingInstruction
}

func NewRegexMasker(maskingConfig []byte) (*RegexMasker, error) {
	var MaskingInstructions []MaskingInstruction
	err := json.Unmarshal(maskingConfig, &MaskingInstructions)
	if err != nil {
		return nil, err
	}

	for i, instruction := range MaskingInstructions {
		MaskingInstructions[i].Regex, err = regexp.Compile(instruction.RegexPattern)
		if err != nil {
			return nil, err
		}
	}

	return &RegexMasker{
		maskingInstructions: MaskingInstructions,
	}, nil
}

func (m *RegexMasker) Mask(content string) string {
	for _, instruction := range m.maskingInstructions {
		content = instruction.Regex.ReplaceAllString(content, instruction.MaskWith)
	}
	return content
}

/*
# Some masking examples
# ---------------------
#
# masking_instances = [
#    MaskingInstruction(r'((?<=[^A-Za-z0-9])|^)(([0-9a-f]{2,}:){3,}([0-9a-f]{2,}))((?=[^A-Za-z0-9])|$)', "clusterID"),
#    MaskingInstruction(r'((?<=[^A-Za-z0-9])|^)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})((?=[^A-Za-z0-9])|$)', "IP"),
#    MaskingInstruction(r'((?<=[^A-Za-z0-9])|^)([0-9a-f]{6,} ?){3,}((?=[^A-Za-z0-9])|$)', "SEQ"),
#    MaskingInstruction(r'((?<=[^A-Za-z0-9])|^)([0-9A-F]{4} ?){4,}((?=[^A-Za-z0-9])|$)', "SEQ"),
#
#    MaskingInstruction(r'((?<=[^A-Za-z0-9])|^)(0x[a-f0-9A-F]+)((?=[^A-Za-z0-9])|$)', "HEX"),
#    MaskingInstruction(r'((?<=[^A-Za-z0-9])|^)([\-\+]?\d+)((?=[^A-Za-z0-9])|$)', "NUM"),
#    MaskingInstruction(r'(?<=executed cmd )(".+?")', "CMD"),
# ]

*/
