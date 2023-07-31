package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToPriceMaster(rows *sql.Rows) (*PriceMaster, error) {
	defer rows.Close()
	priceMaster := PriceMaster{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&priceMaster.SupplyChainRelationshipID,
			&priceMaster.Buyer,
			&priceMaster.Seller,
			&priceMaster.ConditionRecord,
			&priceMaster.ConditionSequentialNumber,
			&priceMaster.Product,
			&priceMaster.ConditionValidityStartDate,
			&priceMaster.ConditionValidityEndDate,
			&priceMaster.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &priceMaster, err
		}

	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &priceMaster, nil
	}

	return &priceMaster, nil
}
