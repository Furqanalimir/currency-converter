package database

import (
	"fmt"
	"sync"
	"gorm.io/gorm"
	"gorm.io/gorm/mysql"
	"reflect"
)

// sql database config model/type
type MysqlConfig struct {
	UserName	string	
	Password	string
	Port		string
	Database	string
	Params		map[string]string
}

type mysqlConnection struct {
	client		*gorm.DB
	transaction *gorm.DB
	mu		    sync.RWMutex
}

func NewSQL()*mysqlConnection{
	return &mysqlConnection{}
}

func (m *mysqlConnection) Connect(config MysqlConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		return fmt.Errorf("database connection already active")
	}

	// TODO: write build uri function
	uri := fmt.Sprintf("%s:%s@tcp(127.0.0.1:%s)/%s?%s", 
    config.UserName, config.Password, config.Port, config.Database, 
    buildQueryParams(config.Params))

	conn, err := gorm.Open(mysql.New(mysql.Config{DNS: uri}))
	if err != nil {
		return fmt.Errorf("failed to connect database: %v", err)
	}

	db, err := conn.DB();
	if err != nil {
		return fmt.Errorf("failed to get database connection: %v", err)
	}

	m.client = db

	return nil
}

func (m *mysqlConnection) beginTransaction(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		return fmt.Errorf("database connection is not active")
	}

	if m.transaction != nil {
		return fmt.Errorf("database transaction is already active")
	}

	db, err := m.client.WithContext(ctx).DB()
	if err != nil {
		return fmt.Errorf("failed to get database connection: %v", err)
	}

	trxn, err := db.Begin();
	if err != nil {
		return fmt.Errorf("failed to initiate database transction: %v", err)
	}

	m.transaction = trxn
	return nil
}

func (m *mysqlConnection) commitTransaction(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		return fmt.Errorf("database connection is not active")
	}

	if m.transaction == nil {
		return fmt.Errorf("database transaction is not active")
	}

	db, err := m.transaction.WithContext(ctx).DB()
	if err != nil {
		return fmt.Errorf("failed to get database connection: %v", err)
	}

	if err := db.WithContext(ctx).Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	m.transaction = nil
	return nil
}

func (m *mysqlConnection) rollbackTransaction(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client != nil {
		return fmt.Errorf("database connection is not active")
	}

	if m.transaction == nil {
		return fmt.Errorf("database transaction is not active")
	}

	db, err := m.transaction.WithContext(ctx).DB()
	if err != nil {
		return fmt.Errorf("failed to get database connection: %v", err)
	}

	if err := db.WithContext(ctx).Rollback(); err != nil {
		return fmt.Errorf("failed to rollback transaction: %v", err)
	}

	m.transaction = nil
	return nil
}

func (m *mysqlConnection) Create(dest interface{}, model interface{}, ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		return fmt.Errorf("database connection is not active")
	}

	if err := m.beginTransaction(ctx); err != nil {
		return fmt.Errorf("failed to initiate transaction: %v", err)
	}

	if err := m.transaction.WithContext(ctx).Model(model).Create(dest).Error; err != nil {
		if trxnErr := m.rollbackTransaction(ctx); trxnErr != nil {
			return trxnErr
		}
		return err
	}

	if err := m.commitTransaction(ctx); err != nil {
		if trxnErr := m.rollbackTransaction(ctx); trxnErr != nil {
			return trxnErr
		}
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	return nil
}

func (m *mysqlConnection) Update(dest, model, conditions interface{}, ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		return fmt.Errorf("database connection is not active")
	}

	if err := m.beginTransaction(ctx); err != nil {
		return fmt.Errorf("failed to initiate transaction: %v", err)
	}

	if err := m.transaction.WithContext(ctx).Updates(dest).Where(conditions).Error; err != nil {
		if trxnErr := m.rollbackTransaction(ctx); trxnErr != nil {
			return trxnErr
		}
		return err
	}

	if err := m.commitTransaction(ctx); err != nil {
		if trxnErr := m.rollbackTransaction(ctx); trxnErr != nil {
			return trxnErr
		}
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

func (m *mysqlConnection) Delete(model, conditions interface{}, ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		return fmt.Errorf("database connection is not active")
	}

	if err := m.beginTransaction(ctx); err != nil {
		return fmt.Errorf("failed to initiate transaction: %v", err)
	}

	if err := m.transaction.WithContext(ctx).Delete(model).Where(conditions).Error; err != nil {
		if trxnErr := m.rollbackTransaction(ctx); trxnErr != nil {
			return trxnErr
		}
		return err
	}

	if err := m.commitTransaction(ctx); err != nil {
		if trxnErr := m.rollbackTransaction(ctx); trxnErr != nil {
			return trxnErr
		}
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	
	return nil
}


func (m *mysqlConnection) FindOne(ctx context.Context, model, conditions interface{}) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		return nil, fmt.Errorf("database connection is not active")
	}

	reflectModel := reflect.TypeOf(model)

	if reflectModel.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("model should be a pointer")
	}

	if reflectModel.Elem().Kind() != reflect.Struct || reflectModel.Elem().Kind() != reflect.Slice{
		return nil, fmt.Errorf("model should be a struct or slice")
	}

	if err := m.client.WithContext(ctx).Where(conditions).First(model).Error; err != nil {
		return nil, err
	}
	
	return model, nil
}

func (m *mysqlConnection) FindAll(ctx context.Context, model, conditions interface{}) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		return nil, fmt.Errorf("database connection is not active")
	}

	reflectModel := reflect.TypeOf(model)

	if reflectModel.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("model should be a pointer")
	}

	if reflectModel.Elem().Kind() != reflect.Slice {
		return nil, fmt.Errorf("model should be a pointer to a slice")
	}

	if err := m.client.WithContext(ctx).Where(conditions).Find(model).Error; err != nil {
		return nil, err
	}

	return model, nil
}

func (m *mysqlConnection) Upsert(ctx context.Context, model interface{}) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		return nil, fmt.Errorf("database connection is not active")
	}

	reflectModel := reflect.TypeOf(model)

	if reflectModel.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("model should be a pointer")
	}

	if reflectModel.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("model should be a pointer to a struct")
	}

	if err := m.beginTransaction(ctx); err != nil {
		return nil, err
	}

	if err := m.transaction.WithContext(ctx).Save(model).Error; err != nil {
		return nil, fmt.Errorf("failed to upsert model: %v", err)
	}

	if err := m.commitTransaction(ctx); err != nil {
		if trxnErr := m.rollbackTransaction(ctx); trxnErr != nil {
			return nil, trxnErr
		}
		return nil, fmt.Errorf("failed to commit transaction: %v", err)
	}

	return model, nil
}
