package migrate

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
)

const defTableName = "schema_migrations"

type Direction int

const DirectionUp Direction = 0
const DirectionDown Direction = 1

type TransactionMode int

const (
	TransactionModeGeneral TransactionMode = iota
	TransactionModeIndividual
)

type Logger interface {
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type Version interface {
	Up(transaction *sqlx.Tx) error
	Down(transaction *sqlx.Tx) error
}

type Collection []Version

func (a *Collection) Add(m Version) {
	*a = append(*a, m)
}

type PassedMigration struct {
	Version   uint64    `db:"version"`
	CreatedAt time.Time `db:"created_at"`
}

type Manager interface {
	Prepare(versions Collection) error
	Up(ctx context.Context, mode TransactionMode) error
	Down(ctx context.Context, mode TransactionMode) error
	Generate() error
	Lookup(versions Collection, versionNames ...string) (Collection, error)
}

type manager struct {
	logger         Logger
	versions       Collection
	conn           *sqlx.DB
	migrationsPath string
	templatePath   string
	tableName      string
}

func NewManager(migrationsDir, templatePath string, logger Logger, tableName string, conn *sqlx.DB) Manager {
	if tableName == "" {
		tableName = defTableName
	}
	return &manager{
		migrationsPath: migrationsDir,
		templatePath:   templatePath,
		logger:         logger,
		tableName:      tableName,
		conn:           conn,
	}
}

func (m *manager) Prepare(versions Collection) error {
	if versions == nil {
		return errors.New("versions must not be nil")
	}
	m.versions = versions
	return nil
}

func (m *manager) Lookup(versions Collection, versionNames ...string) (Collection, error) {
	c := Collection{}
	for _, versionName := range versionNames {
		versionNameInt, err := strconv.ParseUint(versionName, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid version name %s must be timestamp e.g [20210909134215]", versionName)
		}
		for _, version := range versions {
			index, err := versionIndex(version)
			if err != nil {
				return nil, err
			}
			if index == versionNameInt {
				c = append(c, version)
			}
		}
	}
	return c, nil
}

func (m *manager) Up(ctx context.Context, mode TransactionMode) error {
	return m.run(ctx, DirectionUp, mode)
}

func (m *manager) Down(ctx context.Context, mode TransactionMode) error {
	return m.run(ctx, DirectionDown, mode)
}

func (m *manager) Generate() error {
	timestamp := time.Now().Format("20060102150405")
	versionName := fmt.Sprintf("Version%s", timestamp)
	err := os.MkdirAll(m.migrationsPath, 0755)
	if err != nil {
		return err
	}
	fileName := fmt.Sprintf("%s/%s.go", m.migrationsPath, versionName)
	if m.templatePath == "" {
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			panic("No caller information")
		}
		m.templatePath = fmt.Sprintf("%s/version.go.tpl", path.Dir(filename))
	}
	b, err := ioutil.ReadFile(m.templatePath)
	if err != nil {
		return err
	}
	buf := bytes.Buffer{}
	tmpl, err := template.New("migration").Parse(string(b))
	if err != nil {
		return err
	}
	err = tmpl.Execute(&buf, map[string]interface{}{
		"versionName": versionName,
	})
	if err != nil {
		return err
	}
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}

	if err := ioutil.WriteFile(file.Name(), buf.Bytes(), fs.ModeDevice); err != nil {
		return err
	}
	m.logger.Infof("New migration file generated %s", versionName)

	return nil
}

func (m *manager) run(ctx context.Context, direction Direction, mode TransactionMode) (er error) {
	if m.versions == nil {
		return errors.New("versions collection not provided, provide versions collection")
	}
	if err := m.createTableIfNotExists(); err != nil {
		return err
	}
	versions := sortMigrations(m.versions, direction)
	passedMigrations, err := m.findAll()
	if err != nil {
		return err
	}
	passedMigrationsMap := toMap(passedMigrations)
	var tx *sqlx.Tx
	if TransactionModeGeneral == mode {
		tx, err = m.conn.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		if err := m.lockTable(tx); err != nil {
			return err
		}
		defer func(tx *sqlx.Tx, err error) {
			err = tx.Commit()
			if err != nil {
				m.logger.Errorf("Transaction commit err: %s", err)
			}
		}(tx, er)
	}
	for _, version := range versions {
		if TransactionModeIndividual == mode {
			tx, err = m.conn.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
			if err != nil {
				return err
			}
			if err := m.lockTable(tx); err != nil {
				return err
			}
		}
		index, err := versionIndex(version)
		if err != nil {
			return err
		}
		migration := PassedMigration{Version: index, CreatedAt: time.Now()}
		_, passed := passedMigrationsMap[index]
		if direction == DirectionUp {
			if !passed {
				m.logger.Infof("upgrading to version %d...", index)
				if err := version.Up(tx); err != nil {
					return err
				}
				if err := m.insert(tx, migration); err != nil {
					return err
				}
			} else {
				m.logger.Infof("skipping passed version %d...", index)
			}
		} else {
			if passed {
				m.logger.Infof("downgrading version %d...", index)
				if err := version.Down(tx); err != nil {
					return err
				}
				if err := m.delete(tx, migration); err != nil {
					return err
				}
			} else {
				m.logger.Infof("skipping unpassed version %d...", index)
			}
		}
		if TransactionModeIndividual == mode {
			if err := tx.Commit(); err != nil {
				return err
			}
		}

	}
	return nil
}

func sortMigrations(versions []Version, dir Direction) []Version {
	sort.SliceStable(versions, func(i, j int) bool {
		indexI, err := versionIndex(versions[i])
		if err != nil {
			panic(err)
		}
		indexJ, err := versionIndex(versions[j])
		if err != nil {
			panic(err)
		}
		if dir == DirectionUp {
			return indexI < indexJ // up
		}
		return indexI > indexJ // down
	})

	return versions
}

func toMap(passedMigrations []PassedMigration) map[uint64]PassedMigration {
	res := make(map[uint64]PassedMigration, len(passedMigrations))
	for _, m := range passedMigrations {
		res[m.Version] = m
	}
	return res
}

func versionIndex(version Version) (uint64, error) {
	ti := reflect.TypeOf(version)
	return strconv.ParseUint(strings.Replace(ti.Name(), "Version", "", -1), 10, 64)
}

func (m *manager) createTableIfNotExists() error {
	_, err := m.conn.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (version BIGINT UNIQUE, created_at TIMESTAMP);`, m.tableName))
	return err
}

func (m *manager) lockTable(tx *sqlx.Tx) error {
	_, err := tx.Exec(fmt.Sprintf(`LOCK TABLE "%s" IN SHARE MODE;`, m.tableName))
	return err
}

func (m *manager) findAll() ([]PassedMigration, error) {
	var entities []PassedMigration
	query := fmt.Sprintf(`SELECT version, created_at FROM "%s";`, m.tableName)
	rows, err := m.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			m.logger.Infof("migration: rows close err %v", err)
			return
		}
	}()
	for rows.Next() {
		ent := PassedMigration{}
		if err := rows.Scan(&ent.Version, &ent.CreatedAt); err != nil {
			return nil, err
		}
		entities = append(entities, ent)
	}
	return entities, nil
}

func (m *manager) insert(tx *sqlx.Tx, entity PassedMigration) error {
	query := fmt.Sprintf(`INSERT INTO "%s" (version, created_at) VALUES ($1, $2);`, m.tableName)
	_, err := tx.Exec(query, entity.Version, entity.CreatedAt)
	return err
}

func (m *manager) delete(tx *sqlx.Tx, entity PassedMigration) error {
	query := fmt.Sprintf(`DELETE FROM "%s" WHERE version = $1;`, m.tableName)
	_, err := tx.Exec(query, entity.Version)
	return err
}
