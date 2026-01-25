package dbmetadata_test

import (
	"context"
	"os"
	"testing"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbtx"
)

func setupTestViewManager(t *testing.T) (*dbmetadata.ViewManager, *dbtx.Transaction, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "view_manager_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		t.Fatalf("failed to open temp dir: %v", err)
	}

	fm, err := dbfile.NewFileManager(dirFile, 4000) // spanned slotでないため、大きめにとる
	if err != nil {
		t.Fatalf("failed to create file manager: %v", err)
	}

	lm, err := dblog.NewLogManager(fm, "test.log")
	if err != nil {
		t.Fatalf("failed to create log manager: %v", err)
	}

	bm := dbbuffer.NewBufferManager(fm, lm, 8)

	tx, err := dbtx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	ctx := context.Background()
	tm, err := dbmetadata.NewTableManager(ctx, true, tx)
	if err != nil {
		t.Fatalf("failed to create table manager: %v", err)
	}

	vm, err := dbmetadata.NewViewManager(ctx, true, tm, tx)
	if err != nil {
		t.Fatalf("failed to create view manager: %v", err)
	}

	cleanup := func() {
		dirFile.Close()
		os.RemoveAll(dir)
	}

	return vm, tx, cleanup
}

func TestViewManagerCreateViewAndGetViewDef(t *testing.T) {
	vm, tx, cleanup := setupTestViewManager(t)
	defer cleanup()

	ctx := context.Background()

	viewName := "active_users"
	viewDef := "select * from users where active = 1"

	// Create view
	err := vm.CreateView(ctx, viewName, viewDef, tx)
	if err != nil {
		t.Fatalf("failed to create view: %v", err)
	}

	// Get view definition
	retrievedDef, err := vm.GetViewDef(ctx, viewName, tx)
	if err != nil {
		t.Fatalf("failed to get view definition: %v", err)
	}

	if retrievedDef != viewDef {
		t.Errorf("expected view definition %q, got %q", viewDef, retrievedDef)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestViewManagerMultipleViews(t *testing.T) {
	vm, tx, cleanup := setupTestViewManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create first view
	view1Name := "expensive_products"
	view1Def := "select * from products where price > 100"

	err := vm.CreateView(ctx, view1Name, view1Def, tx)
	if err != nil {
		t.Fatalf("failed to create first view: %v", err)
	}

	// Create second view
	view2Name := "recent_orders"
	view2Def := "select * from orders where date > '2024-01-01'"

	err = vm.CreateView(ctx, view2Name, view2Def, tx)
	if err != nil {
		t.Fatalf("failed to create second view: %v", err)
	}

	// Verify first view
	def1, err := vm.GetViewDef(ctx, view1Name, tx)
	if err != nil {
		t.Fatalf("failed to get first view definition: %v", err)
	}
	if def1 != view1Def {
		t.Errorf("expected first view definition %q, got %q", view1Def, def1)
	}

	// Verify second view
	def2, err := vm.GetViewDef(ctx, view2Name, tx)
	if err != nil {
		t.Fatalf("failed to get second view definition: %v", err)
	}
	if def2 != view2Def {
		t.Errorf("expected second view definition %q, got %q", view2Def, def2)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestViewManagerGetNonexistentView(t *testing.T) {
	vm, tx, cleanup := setupTestViewManager(t)
	defer cleanup()

	ctx := context.Background()

	// Try to get a view that doesn't exist
	def, err := vm.GetViewDef(ctx, "nonexistent_view", tx)
	if err != nil {
		t.Fatalf("failed to get view definition: %v", err)
	}

	// Should return empty string
	if def != "" {
		t.Errorf("expected empty string for nonexistent view, got %q", def)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func TestViewManagerLongViewDefinition(t *testing.T) {
	vm, tx, cleanup := setupTestViewManager(t)
	defer cleanup()

	ctx := context.Background()

	viewName := "complex_view"
	// Create a view definition that's close to the max length (100 characters)
	viewDef := "select id, name, email from users where status = 'active' and created_at > '2024-01-01' limit 50"

	err := vm.CreateView(ctx, viewName, viewDef, tx)
	if err != nil {
		t.Fatalf("failed to create view with long definition: %v", err)
	}

	retrievedDef, err := vm.GetViewDef(ctx, viewName, tx)
	if err != nil {
		t.Fatalf("failed to get view definition: %v", err)
	}

	if retrievedDef != viewDef {
		t.Errorf("expected view definition %q, got %q", viewDef, retrievedDef)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
