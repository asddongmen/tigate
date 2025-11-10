package dhaccountdetails

import (
	"bytes"
	"fmt"
	"math/rand"

	"workload/schema"
)

const createAccountDetailsTable = `
CREATE TABLE IF NOT EXISTS dh_account_details_%[1]d (
  c_id bigint NOT NULL,
  c_site varchar(64) NOT NULL DEFAULT '',
  c_created_at int NOT NULL DEFAULT 0,
  c_created_at_ms bigint NOT NULL DEFAULT 0,
  c_user bigint NOT NULL DEFAULT 0,
  c_register_type int NOT NULL DEFAULT 0,
  c_record varchar(255) NOT NULL DEFAULT '',
  c_game_type int NOT NULL DEFAULT 0,
  c_game_category int NOT NULL DEFAULT 0,
  c_metric decimal(20,6) NOT NULL DEFAULT 0,
  c_amount decimal(20,6) NOT NULL DEFAULT 0.000000,
  c_currency varchar(64) NOT NULL DEFAULT '',
  c_deal_type int NOT NULL DEFAULT 0,
  c_withdraw varchar(128) NOT NULL DEFAULT '',
  c_active_id int NOT NULL DEFAULT 0,
  c_remark varchar(4096) NOT NULL DEFAULT '',
  c_remark_back varchar(2048) NOT NULL DEFAULT '',
  c_remark_front varchar(2048) NOT NULL DEFAULT '',
  c_operator varchar(64) NOT NULL DEFAULT '',
  c_operator_create varchar(64) NOT NULL DEFAULT '',
  c_operator_pay varchar(64) NOT NULL DEFAULT '',
  c_operator_review varchar(64) NOT NULL DEFAULT '',
  c_operator_back varchar(64) NOT NULL DEFAULT '',
  c_operator_front varchar(64) NOT NULL DEFAULT '',
  c_opt_type int NOT NULL DEFAULT 0,
  c_game_id int NOT NULL DEFAULT 0,
  c_audit_status int NOT NULL DEFAULT 0,
  c_remaining decimal(20,6) NOT NULL DEFAULT 0,
  c_audit_rate decimal(20,6) NOT NULL DEFAULT 0,
  c_audit_order varchar(64) NOT NULL DEFAULT '',
  c_demand decimal(20,6) NOT NULL DEFAULT 0,
  c_active_status varchar(2048) NOT NULL DEFAULT '',
  c_biz_id varchar(200) NOT NULL DEFAULT '',
  c_bonus_status int NOT NULL DEFAULT 0,
  c_bonus_time int NOT NULL DEFAULT 0,
  c_wallet_type int NOT NULL DEFAULT 1,
  PRIMARY KEY (c_id, c_site),
  KEY idx_c_game_type (c_game_type),
  KEY idx_c_active_id (c_active_id),
  KEY idx_c_game_id (c_game_id),
  KEY idx_c_audit_order_id (c_audit_order, c_id),
  KEY idx_c_created_at (c_created_at),
  KEY idx_c_created_at_ms (c_created_at_ms),
  KEY idx_c_user_deal_opt_metric_created (c_user, c_deal_type, c_opt_type, c_metric, c_created_at),
  KEY idx_c_metric_created (c_metric, c_created_at),
  KEY idx_c_user_opt_created (c_user, c_opt_type, c_created_at),
  KEY idx_c_user_game_opt_created (c_user, c_game_type, c_opt_type, c_created_at),
  KEY idx_c_deal_opt_created_ms (c_deal_type, c_opt_type, c_created_at_ms),
  KEY idx_c_user_deal_created (c_user, c_deal_type, c_created_at),
  KEY idx_c_register_created_ms (c_register_type, c_created_at_ms),
  UNIQUE KEY uk_c_biz_site (c_biz_id, c_site),
  KEY idx_c_opt_site_created (c_opt_type, c_site, c_created_at),
  KEY idx_c_record_site (c_record, c_site),
  KEY idx_c_site_created (c_site, c_created_at),
  KEY idx_c_site_created_ms (c_site, c_created_at_ms),
  KEY idx_c_user_deal_opt_metric_site_created (c_user, c_deal_type, c_opt_type, c_metric, c_site, c_created_at),
  KEY idx_c_metric_site_created (c_metric, c_site, c_created_at),
  KEY idx_c_user_opt_site_created (c_user, c_opt_type, c_site, c_created_at),
  KEY idx_c_user_game_opt_site_created (c_user, c_game_type, c_opt_type, c_site, c_created_at),
  KEY idx_c_opt_site_created_ms (c_opt_type, c_site, c_created_at_ms),
  KEY idx_c_user_deal_site_created (c_user, c_deal_type, c_site, c_created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='synthetic account details'
`

const accountDetailsColumns = "c_id,c_site,c_created_at,c_created_at_ms,c_user,c_register_type,c_record,c_game_type,c_game_category,c_metric,c_amount,c_currency,c_deal_type,c_withdraw,c_active_id,c_remark,c_remark_back,c_remark_front,c_operator,c_operator_create,c_operator_pay,c_operator_review,c_operator_back,c_operator_front,c_opt_type,c_game_id,c_audit_status,c_remaining,c_audit_rate,c_audit_order,c_demand,c_active_status,c_biz_id,c_bonus_status,c_bonus_time,c_wallet_type"

const (
	fixedCreatedAt               = 1700000000
	fixedCreatedAtMS       int64 = 1700000000000
	fixedUser                    = 100000
	fixedRegisterType            = 1
	fixedRecord                  = "record-fixed"
	fixedGameType                = 10
	fixedGameCategory            = 5
	fixedMetric                  = "100.000000"
	fixedAmount                  = "200.000000"
	fixedCurrency                = "USD"
	fixedDealType                = 2
	fixedWithdraw                = "bank"
	fixedActiveID                = 100
	fixedRemark                  = "remark-default"
	fixedRemarkBack              = "remark-back"
	fixedRemarkFront             = "remark-front"
	fixedOperator                = "operator"
	fixedOperatorCreate          = "operator-create"
	fixedOperatorPay             = "operator-pay"
	fixedOperatorReview          = "operator-review"
	fixedOperatorBack            = "operator-back"
	fixedOperatorFront           = "operator-front"
	fixedOptType                 = 3
	fixedGameID                  = 200
	fixedAuditStatus             = 1
	fixedRemaining               = "50.000000"
	fixedAuditRate               = "75.000000"
	fixedAuditOrder              = "audit-order"
	fixedDemand                  = "120.000000"
	fixedActiveStatus            = "ACTIVE"
	fixedBizID                   = "biz-001"
	fixedBonusStatus             = 0
	fixedBonusTime               = 1700000000
	fixedWalletType              = 1
	fixedRemarkUpdated           = "remark-updated"
	fixedRemarkBackUpdated       = "remark-back-updated"
)

type AccountDetailsWorkload struct{}

func NewAccountDetailsWorkload() schema.Workload {
	return &AccountDetailsWorkload{}
}

func (w *AccountDetailsWorkload) BuildCreateTableStatement(n int) string {
	return fmt.Sprintf(createAccountDetailsTable, n)
}

func (w *AccountDetailsWorkload) BuildInsertSql(tableN int, batchSize int) string {
	tableName := fmt.Sprintf("dh_account_details_%d", tableN)
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES", tableName, accountDetailsColumns))

	for i := 0; i < batchSize; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("(")
		buf.WriteString(w.buildRowValues())
		buf.WriteString(")")
	}
	return buf.String()
}

func (w *AccountDetailsWorkload) BuildUpdateSql(opt schema.UpdateOption) string {
	tableName := fmt.Sprintf("dh_account_details_%d", opt.TableIndex)
	var buf bytes.Buffer
	for i := 0; i < opt.Batch; i++ {
		if i > 0 {
			buf.WriteString(";")
		}
		siteCode := rand.Intn(512)
		buf.WriteString(fmt.Sprintf(
			"UPDATE %s SET c_amount = c_amount + 1.000000, c_metric = c_metric + 1.000000, c_remark = '%s', c_remark_back = '%s' WHERE c_site = '%d' LIMIT 1",
			tableName,
			fixedRemarkUpdated,
			fixedRemarkBackUpdated,
			siteCode,
		))
	}
	return buf.String()
}

func (w *AccountDetailsWorkload) BuildDeleteSql(opts schema.DeleteOption) string {
	tableName := fmt.Sprintf("dh_account_details_%d", opts.TableIndex)
	switch rand.Intn(3) {
	case 0:
		return fmt.Sprintf("DELETE FROM %s WHERE c_site = '%d' LIMIT %d", tableName, rand.Intn(512), opts.Batch)
	case 1:
		return fmt.Sprintf("DELETE FROM %s WHERE c_user = %d LIMIT %d", tableName, rand.Int63(), opts.Batch)
	default:
		return fmt.Sprintf("DELETE FROM %s WHERE c_opt_type = %d AND c_deal_type = %d LIMIT %d", tableName, rand.Intn(101), rand.Intn(2000), opts.Batch)
	}
}

func (w *AccountDetailsWorkload) buildRowValues() string {
	id := rand.Int63()
	siteCode := rand.Intn(512)

	return fmt.Sprintf(
		"%d,'%d',%d,%d,%d,%d,'%s',%d,%d,%s,%s,'%s',%d,'%s',%d,'%s','%s','%s','%s','%s','%s','%s','%s','%s',%d,%d,%d,%s,%s,'%s',%s,'%s','%s',%d,%d,%d",
		id,
		siteCode,
		fixedCreatedAt,
		fixedCreatedAtMS,
		fixedUser,
		fixedRegisterType,
		fixedRecord,
		fixedGameType,
		fixedGameCategory,
		fixedMetric,
		fixedAmount,
		fixedCurrency,
		fixedDealType,
		fixedWithdraw,
		fixedActiveID,
		fixedRemark,
		fixedRemarkBack,
		fixedRemarkFront,
		fixedOperator,
		fixedOperatorCreate,
		fixedOperatorPay,
		fixedOperatorReview,
		fixedOperatorBack,
		fixedOperatorFront,
		fixedOptType,
		fixedGameID,
		fixedAuditStatus,
		fixedRemaining,
		fixedAuditRate,
		fixedAuditOrder,
		fixedDemand,
		fixedActiveStatus,
		fixedBizID,
		fixedBonusStatus,
		fixedBonusTime,
		fixedWalletType,
	)
}
