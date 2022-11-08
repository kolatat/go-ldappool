package adhelper

import (
	"fmt"

	"github.com/go-ldap/ldap/v3"
	"golang.org/x/text/encoding/unicode"
)

var (
	utf16le = unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewEncoder()
)

func ResetPassword(modifyRequest *ldap.ModifyRequest, password string) error {
	// https://gist.github.com/Project0/61c13130563cf7f595e031d54fe55aab
	// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-adts/3c5e87db-4728-4f29-b164-01dd7d7391ea
	encPwd, err := utf16le.String(fmt.Sprintf("%q", password))
	if err != nil {
		return err
	}
	modifyRequest.Replace("unicodePwd", []string{encPwd})
	return nil
}
