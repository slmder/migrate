package migrations

import (
	"github.com/jmoiron/sqlx"
)

type {{.versionName}} struct {
}

func New{{.versionName}}() {{.versionName}} {
	return {{.versionName}}{}
}

func (v {{.versionName}}) Up(tx *sqlx.Tx) error {
	_, err := tx.Exec(`SELECT 1`)
	return err
}

func (v {{.versionName}}) Down(tx *sqlx.Tx) error {
	_, err := tx.Exec(`SELECT 1`)
	return err
}
