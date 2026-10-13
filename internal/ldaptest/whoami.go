package ldaptest

import (
	ber "github.com/go-asn1-ber/asn1-ber"
	msg "github.com/vjeantet/goldap/message"
)

// whoAmIResponse builds a "who am I?" extended response carrying value as its
// response value (RFC 4532). The goldap ExtendedResponse type has no exported
// setter for the value, so the response is encoded as raw BER, wrapped in an
// LDAP message envelope and parsed back with goldap's own reader.
func whoAmIResponse(value string) (msg.ExtendedResponse, error) {
	resp := ber.Encode(ber.ClassApplication, ber.TypeConstructed, msg.TagExtendedResponse, nil, "")
	resp.AppendChild(ber.NewInteger(ber.ClassUniversal, ber.TypePrimitive, ber.TagEnumerated, 0, ""))
	resp.AppendChild(ber.NewString(ber.ClassUniversal, ber.TypePrimitive, ber.TagOctetString, "", ""))
	resp.AppendChild(ber.NewString(ber.ClassUniversal, ber.TypePrimitive, ber.TagOctetString, "", ""))
	resp.AppendChild(ber.NewString(ber.ClassContext, ber.TypePrimitive, msg.TagExtendedResponseValue, value, ""))
	env := ber.Encode(ber.ClassUniversal, ber.TypeConstructed, ber.TagSequence, nil, "")
	env.AppendChild(ber.NewInteger(ber.ClassUniversal, ber.TypePrimitive, ber.TagInteger, 1, ""))
	env.AppendChild(resp)

	parsed, err := msg.ReadLDAPMessage(msg.NewBytes(0, env.Bytes()))
	if err != nil {
		return msg.ExtendedResponse{}, err
	}
	return parsed.ProtocolOp().(msg.ExtendedResponse), nil
}
