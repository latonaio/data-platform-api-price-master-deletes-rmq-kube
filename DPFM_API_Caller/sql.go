package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-price-master-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-price-master-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	"strings"
)

func (c *DPFMAPICaller) PriceMasterRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.PriceMaster {

	where := strings.Join([]string{
		fmt.Sprintf("WHERE priceMaster.SupplyChainRelationshipID = %d ", input.PriceMaster.SupplyChainRelationshipID),
		fmt.Sprintf("AND priceMaster.Buyer = %d ", input.PriceMaster.Buyer),
		fmt.Sprintf("AND priceMaster.Seller = %d ", input.PriceMaster.Seller),
		fmt.Sprintf("AND priceMaster.ConditionRecord = %d ", input.PriceMaster.ConditionRecord),
		fmt.Sprintf("AND priceMaster.ConditionSequentialNumber = %d ", input.PriceMaster.ConditionSequentialNumber),
		fmt.Sprintf("AND priceMaster.Product = \"%s\" ", string(input.PriceMaster.Product)),
		fmt.Sprintf("AND priceMaster.ConditionValidityStartDate = \"%s\" ", input.PriceMaster.ConditionValidityStartDate),
		fmt.Sprintf("AND priceMaster.ConditionValidityEndDate = \"%s\" ", input.PriceMaster.ConditionValidityEndDate),
	}, "")

	rows, err := c.db.Query(
		`SELECT 
    	priceMaster.SupplyChainRelationshipID,
    	priceMaster.Buyer,
    	priceMaster.Seller,
    	priceMaster.ConditionRecord,
    	priceMaster.ConditionSequentialNumber,
    	priceMaster.Product,
    	priceMaster.ConditionValidityStartDate,
    	priceMaster.ConditionValidityEndDate
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_price_master_price_master_data as priceMaster 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToPriceMaster(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
