package service

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/NewStreetTechnologies/go-backend-common/logger"

	"github.com/NewStreetTechnologies/darwin-bank-module/config"
	"github.com/NewStreetTechnologies/darwin-bank-module/graph"
	"github.com/NewStreetTechnologies/darwin-bank-module/helpers"
	"github.com/NewStreetTechnologies/darwin-bank-module/integrations"
	"github.com/NewStreetTechnologies/darwin-bank-module/middlewares"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/cif"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/common"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/customerutility"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/dedupe"
	disbursementenquiry "github.com/NewStreetTechnologies/darwin-bank-module/models/disbursementEnquiry"
	disbursementinitiation "github.com/NewStreetTechnologies/darwin-bank-module/models/disbursementInitiation"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/ekyc"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/fundTransferStatus"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/imps"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/loanRenewalFlow"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/loanstatus"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/lrBorrowDetails"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/lrLatestInterestRate"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/lrNextInterestDate"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/lrSanctionLimitAndDetailsUpd"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/nameAndDob"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/queue"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/sbAccountCreation"
	"github.com/NewStreetTechnologies/darwin-bank-module/models/unNameCheck"
	_logger "github.com/NewStreetTechnologies/darwin-bank-module/pkg/logger"
	"github.com/NewStreetTechnologies/darwin-bank-module/repository"
	"github.com/NewStreetTechnologies/darwin-bank-module/utils"
)

// CommonService provides common functionalities for the service.
type CommonService struct {
	CommonRepository   repository.CommonRepository
	CommonIntegrations integrations.CommonIntegrations
	CommonGraph        graph.CommonGraph
	Logger             *logger.Log
}

// InitCommonService initializes the CommonService.
func InitCommonService(commonRepo repository.CommonRepository) *CommonService {
	commonIntegrations := integrations.InitCommonIntegrations()
	commonGraph := graph.InitCommonGraph(commonRepo)
	return &CommonService{
		CommonRepository:   commonRepo,
		CommonIntegrations: *commonIntegrations,
		CommonGraph:        *commonGraph,
		Logger:             _logger.Logger,
	}
}

// RequestProcessor type RequestProcessor
type RequestProcessor func(cs *CommonService, queueData *common.GetRequestResponse) (*common.IntegrationResponse, error)

var requestProcessors = map[string]RequestProcessor{
	config.CifCreationId:             (*CommonService).ProcessCifCreationRequest,
	config.CifEnquiryId:              (*CommonService).ProcessCifEnquiryRequest,
	config.IntraFundTransferId:       (*CommonService).ProcessIntraFTFundTransferRequest,
	config.LrIntraFundTransferId:     (*CommonService).ProcessIntraFTFundTransferRequest,
	config.DedupeCheckId:             (*CommonService).ProcessDedupeRequest,
	config.LrDedupeCheckId:           (*CommonService).ProcessDedupeRequest,
	config.DisbursementInitId:        (*CommonService).ProcessDisbursalInitRequest,
	config.DisbursementEnqId:         (*CommonService).ProcessDisbursalEnqRequest,
	config.CustomUtilityId:           (*CommonService).ProcessCustomerUtilityRequest,
	config.LrCustomerUtilityId:       (*CommonService).ProcessCustomerUtilityRequest,
	config.EkycDataPullId:            (*CommonService).ProcessEKycDataPullRequest,
	config.LoanAccEnquiryStatus:      (*CommonService).ProcessLoanAccEnquiryRequest,
	config.PanValidation:             (*CommonService).ProcessPanValidationRequest,
	config.NameAndDobId:              (*CommonService).ProcessNameAndDobRequest,
	config.NameAndDobInd:             (*CommonService).ProcessNameAndDobRequest,
	config.LrNameAndDobId:            (*CommonService).ProcessNameAndDobRequest,
	config.SbAccCreationId:           (*CommonService).ProcessSbAccountCreationRequest,
	config.LrSbAccCreationId:         (*CommonService).ProcessSbAccountCreationRequest,
	config.SbEnquiryId:               (*CommonService).ProcessSbAccountEnquiryRequest,
	config.LrSbEnquiryId:             (*CommonService).ProcessSbAccountEnquiryRequest,
	config.LoanAccCreationId:         (*CommonService).ProcessLoanAccountCreationRequest,
	config.UnNameCheckId:             (*CommonService).ProcessUnNameCheckRequest,
	config.CollateralLodge:           (*CommonService).ProcessCollateralLodgeRequest,
	config.CollateralLinkId:          (*CommonService).ProcessCollateralLinkRequest,
	config.LoanCreationId:            (*CommonService).ProcessLoanAccountCreationRequest,
	config.LrBorrowDetailsUpd:        (*CommonService).ProcesssLrBorrowDetailsUpdateRequest,
	config.LrLatestInterestRateUpd:   (*CommonService).ProcessLrLatestInterestRateUpdRequest,
	config.LrNextInterestDateUpd:     (*CommonService).ProcessLrNextInterestDateUpdRequest,
	config.LrSanctionLimitAndDtlsUpd: (*CommonService).ProcessLrSanctionLimitAndDtlsUpdRequst,
	config.CoCifCreationId:           (*CommonService).ProcessCoApplicantCifCreationRequest,
	config.CoCifEnquiryId:            (*CommonService).ProcessCifEnquiryRequest,
}

func (cs *CommonService) QueueHandlers(message []byte) (*common.IntegrationResponse, error) {
	interceptor := middlewares.NewInterceptor(cs.Logger)
	decryptedPayload, err := interceptor.DecryptQueueRequest(string(message))
	if err != nil {
		cs.Logger.Errorln("[QueueHandlers] ", config.GetErrorCode("DRBM0054", config.CommonErrorCodes))
		return nil, err
	}

	responseInBytes, err := json.Marshal(decryptedPayload)
	if err != nil {
		return nil, err
	}
	queueData, err := cs.GetRequestData(responseInBytes)
	if err != nil {
		return nil, err
	}
	if queueData == nil {
		return nil, nil
	}
	switch {
	case queueData.RequestName == config.IMPSId || queueData.RequestName == config.LrIMPSId || queueData.RequestName == config.PennyDrop || queueData.RequestName == config.LrPennyDrop || queueData.RequestName == config.InsuranceTransfer:
		// Process IMPS request
		return cs.ProcessIMPSFundTransferRequest(queueData)
	case queueData.RequestName == config.NEFTFundTransferId || queueData.RequestName == config.LrNEFTFundTransferId || queueData.RequestName == config.NeftPennyDrop || queueData.RequestName == config.LrNeftPennyDrop:
		// Process NEFT request
		return cs.ProcessNEFTFundTransferRequest(queueData)
	case queueData.RequestName == config.FundTransferStatusId || queueData.RequestName == config.LrFundTransferStatusId || queueData.RequestName == config.InsuranceFTEnquiry:
		// Process FT Enquiry request
		return cs.ProcessFundTransferStatusRequest(queueData)
	case queueData.RequestName == "loan_acc_creation_trigger":
		err = cs.triggerDisbursement(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.RequestName)
		if err != nil {
			return nil, err
		}
		return nil, nil
	default:
		// Invalid request name
		processor, found := requestProcessors[queueData.RequestName]
		if !found {
			cs.Logger.Errorln("[QueueHandlers] [Missing request name: "+queueData.RequestName+"] ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, config.GetErrorCode("DRBM0019", config.CommonErrorCodes))
			return nil, config.GetErrorCode("DRBM0019", config.CommonErrorCodes)
		}
		return processor(cs, queueData)
	}
}

func (cs *CommonService) ProcessDedupeRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {

	if strings.HasPrefix(queueData.CustomerId, config.CoApplicantPrefix) {

		requestDelayStr := os.Getenv("REQUEST_DELAY")
		requestDelay, _ := strconv.Atoi(requestDelayStr)
		time.Sleep(time.Duration(requestDelay) * time.Second)

		applicantId, err := cs.CommonRepository.GetApplicantId(queueData.CustomerId, queueData.ProductCode)
		if err != nil {
			return nil, err
		}

		if len(applicantId) == 0 {
			return nil, fmt.Errorf("applicant not found for the customer %s, requestName: %s", queueData.CustomerId, queueData.RequestName)
		}

		apiData, err := cs.CommonRepository.CheckApiStatus(applicantId, queueData.ProductCode, config.LifecycleLevelMapper["dedupe_check"])
		if err != nil {
			return nil, err
		}

		if apiData != config.SuccessStatus {
			return nil, fmt.Errorf("API status is not success for applicant: %s, requestName: %s", queueData.CustomerId, queueData.RequestName)
		}
	}

	var resp *common.IntegrationResponse
	var dedupeData dedupe.DedupePayloadQueueData
	if err := json.Unmarshal(queueData.JsonData, &dedupeData); err != nil {
		cs.Logger.Error("[ProcessDedupeRequest] (JsonData) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	return cs.DedupeService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, queueData.RequestName, dedupeData)
}

func (cs *CommonService) ProcessDisbursalInitRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var resp *common.IntegrationResponse
	var dataInQueue queue.DataPresentInQueue
	var disbursalInitPayloadData disbursementinitiation.DisbursalInitPayloadData
	if err := json.Unmarshal(queueData.JsonData, &disbursalInitPayloadData); err != nil {
		cs.Logger.Error("[ProcessDisbursalInitRequest] (JsonData -> DisbursalInitPayloadData) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessDisbursalInitRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	apiFlag, err := utils.CheckPrevAPIStatusInLifecycle(queueData.CustomerId, queueData.RequestName, queueData.ProductCode, queueData.DlqFlag)
	if err != nil {
		return nil, err
	}
	if !apiFlag {
		//retrigger the previous API
		return nil, config.GetErrorCode("DRBM0064", config.CommonErrorCodes)
	}
	return cs.DisbursalInitService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, dataInQueue)
}

func (cs *CommonService) ProcessDisbursalEnqRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var resp *common.IntegrationResponse
	var dataInQueue queue.DataPresentInQueue
	var disbursalEnqQueueData disbursementenquiry.DisbursalEnqQueueData
	if err := json.Unmarshal(queueData.JsonData, &disbursalEnqQueueData); err != nil {
		cs.Logger.Error("[ProcessDisbursalEnqRequest] (JsonData -> DisbursalEnqQueueData) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessDisbursalEnqRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	dataInQueue.DlqFlag = queueData.DlqFlag
	return cs.DisbursalEnquiryService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, dataInQueue, disbursalEnqQueueData)
}

func (cs *CommonService) ProcessIMPSFundTransferRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var resp *common.IntegrationResponse
	var impsResponse imps.IMPSPayloadQueueData
	var dataInQueue queue.DataPresentInQueue
	if err := json.Unmarshal(queueData.JsonData, &impsResponse); err != nil {
		cs.Logger.Error("[ProcessIMPSFundTransferRequest] (JsonData -> IMPSPayloadQueueData) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessIMPSFundTransferRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	return cs.IMPSService(queueData.RequestName, queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.RoleCode, queueData.ReferenceId, impsResponse, dataInQueue)
}

func (cs *CommonService) ProcessFundTransferStatusRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var resp *common.IntegrationResponse
	var dataInQueue queue.DataPresentInQueue
	var fundTransferStatusReq fundTransferStatus.FundTransferStatusPayload
	if err := json.Unmarshal(queueData.JsonData, &fundTransferStatusReq); err != nil {
		cs.Logger.Error("[ProcessFundTransferStatusRequest] (JsonData -> FundTransferStatusPayload) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessFundTransferStatusRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	dataInQueue.DlqFlag = queueData.DlqFlag
	return cs.FundTransferStatusService(queueData.RequestName, queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, dataInQueue, fundTransferStatusReq)
}

func (cs *CommonService) ProcessCifCreationRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var response *common.IntegrationResponse
	var cifData cif.CifPayloadData
	if err := json.Unmarshal(queueData.JsonData, &cifData); err != nil {
		cs.Logger.Error("[ProcessCifCreationRequest] (JsonData -> CifPayloadData) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}

	var dataPresentInQueue queue.DataPresentInQueue
	if err := json.Unmarshal(queueData.JsonData, &dataPresentInQueue); err != nil {
		cs.Logger.Error("[ProcessCifCreationRequest] (JsonData -> CifPayloadData) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}

	apiFlag, err := utils.CheckPrevAPIStatusInLifecycle(queueData.CustomerId, queueData.RequestName, queueData.ProductCode, queueData.DlqFlag)
	if err != nil {
		return nil, err
	}
	if !apiFlag {
		//retrigger the previous API
		return nil, config.GetErrorCode("DRBM0064", config.CommonErrorCodes)
	}
	return cs.CifCreationService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, cifData, config.CifCreationId, dataPresentInQueue)
}

func (cs *CommonService) ProcessCifEnquiryRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var response *common.IntegrationResponse
	var cifResponse cif.CIFEnquiryPayload
	var dataInQueue queue.DataPresentInQueue
	if err := json.Unmarshal(queueData.JsonData, &cifResponse); err != nil {
		cs.Logger.Error("[ProcessCifEnquiryRequest] (JsonData -> CIFEnquiryPayload) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessCifEnquiryRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	dataInQueue.DlqFlag = queueData.DlqFlag
	return cs.CifStatusEnquiryService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, cifResponse, dataInQueue, queueData.RequestName)
}

func (cs *CommonService) ProcessNEFTFundTransferRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var response *common.IntegrationResponse
	var dataInQueue queue.DataPresentInQueue
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessNEFTFundTransferRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	return cs.NEFTInitiationService(queueData.RequestName, queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, dataInQueue)
}

func (cs *CommonService) ProcessIntraFTFundTransferRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var response *common.IntegrationResponse
	var dataInQueue queue.DataPresentInQueue
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessIntraFTFundTransferRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	return cs.IntraFTInitiationService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, dataInQueue, queueData.RequestName)
}
func (cs *CommonService) ProcessPanValidationRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var response *common.IntegrationResponse
	var dataInQueue queue.DataPresentInQueue
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessPanValidationRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	return cs.PanValidationService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, dataInQueue)
}

func (cs *CommonService) ProcessCustomerUtilityRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var response *common.IntegrationResponse
	var customerUtilityResponse customerutility.CustomUtilityPayloadData
	if err := json.Unmarshal(queueData.JsonData, &customerUtilityResponse); err != nil {

		cs.Logger.Error("[ProcessCustomerUtilityRequest] (JsonData) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	return cs.CustomUtilityService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, customerUtilityResponse.CustomerID, queueData.RequestName)
}

func (cs *CommonService) ProcessEKycDataPullRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var response *common.IntegrationResponse
	var eKycDataPullResponse ekyc.EkycPayloadData
	if err := json.Unmarshal(queueData.JsonData, &eKycDataPullResponse); err != nil {
		cs.Logger.Error("[ProcessEKycDataPullRequest] (JsonData -> EkycPayloadData) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}

	return cs.EKycDataPullService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, eKycDataPullResponse.TransactionID)
}

func (cs *CommonService) ProcessLoanAccEnquiryRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var response *common.IntegrationResponse
	var loanEnquiryResponse loanstatus.LoanEnquiryQueuePayloadData
	var dataInQueue queue.DataPresentInQueue
	if err := json.Unmarshal(queueData.JsonData, &loanEnquiryResponse); err != nil {
		cs.Logger.Error("[ProcessLoanAccEnquiryRequest] (JsonData -> LoanEnquiryQueuePayloadData) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessLoanAccEnquiryRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	dataInQueue.DlqFlag = queueData.DlqFlag
	return cs.LoanAccEnquiryStatusService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, loanEnquiryResponse, dataInQueue)
}

func (cs *CommonService) ProcessLoanAccountCreationRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var response *common.IntegrationResponse
	var dataInQueue queue.DataPresentInQueue

	if strings.HasPrefix(queueData.CustomerId, config.CoApplicantPrefix) {
		custId, err := cs.CommonRepository.GetApplicantId(queueData.CustomerId, queueData.ProductCode)
		if err != nil {
			return nil, err
		}
		queueData.CustomerId = custId
	}
	// TODO: replace coapplicant id with applicant id
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessLoanAccountCreationRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}

	// apiFlag, err := utils.CheckPrevAPIStatusInLifecycle(queueData.CustomerId, config.LoanAccCreationId, queueData.ProductCode, queueData.DlqFlag)
	// if err != nil {
	// 	return nil, err
	// }
	// if !apiFlag {
	// 	//retrigger the previous API
	// 	return nil, config.GetErrorCode("DRBM0064", config.CommonErrorCodes)
	// }
	return cs.LoanAccCreationService(queueData.RequestName, queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, dataInQueue)
}

func (cs *CommonService) ProcessNameAndDobRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	if strings.HasPrefix(queueData.CustomerId, config.CoApplicantPrefix) {
		requestDelayStr := os.Getenv("REQUEST_DELAY")
		requestDelay, _ := strconv.Atoi(requestDelayStr)
		time.Sleep(time.Duration(requestDelay) * time.Second)

		applicantId, err := cs.CommonRepository.GetApplicantId(queueData.CustomerId, queueData.ProductCode)
		if err != nil {
			return nil, err
		}

		if len(applicantId) == 0 {
			return nil, fmt.Errorf("applicant not found for the customer %s, requestName: %s", queueData.CustomerId, queueData.RequestName)
		}

		apiData, err := cs.CommonRepository.CheckApiStatus(applicantId, queueData.ProductCode, config.LifecycleLevelMapper["name_dob"])
		if err != nil {
			return nil, err
		}

		if apiData != config.SuccessStatus {
			return nil, fmt.Errorf("API status is not success for applicant: %s, requestName: %s", queueData.CustomerId, queueData.RequestName)
		}
	}

	var response *common.IntegrationResponse
	var nameAndDobData nameAndDob.NameAndDobFromQueue
	if err := json.Unmarshal(queueData.JsonData, &nameAndDobData); err != nil {
		cs.Logger.Error("[ProcessNameAndDobRequest] (JsonData) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	if nameAndDobData.CustomerId == "" && queueData.CustomerId != "" {
		nameAndDobData.CustomerId = queueData.CustomerId
	}
	return cs.NameAndDobService(queueData.RequestName, queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, nameAndDobData)
}

func (cs *CommonService) ProcessUnNameCheckRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {

	if strings.HasPrefix(queueData.CustomerId, config.CoApplicantPrefix) {
		requestDelayStr := os.Getenv("REQUEST_DELAY")
		requestDelay, _ := strconv.Atoi(requestDelayStr)
		time.Sleep(time.Duration(requestDelay) * time.Second)

		applicantId, err := cs.CommonRepository.GetApplicantId(queueData.CustomerId, queueData.ProductCode)
		if err != nil {
			return nil, err
		}

		if len(applicantId) == 0 {
			return nil, fmt.Errorf("applicant not found for the customer %s, requestName: %s", queueData.CustomerId, queueData.RequestName)
		}

		apiData, err := cs.CommonRepository.CheckApiStatus(applicantId, queueData.ProductCode, config.LifecycleLevelMapper["un_check"])
		if err != nil {
			return nil, err
		}

		if apiData != config.SuccessStatus {
			return nil, fmt.Errorf("API status is not success for applicant: %s, requestName: %s", queueData.CustomerId, queueData.RequestName)
		}
	}

	var response *common.IntegrationResponse
	var unNameCheckData unNameCheck.UnNameCheckDataFromQueue
	var dataInQueue queue.DataPresentInQueue
	if err := json.Unmarshal(queueData.JsonData, &unNameCheckData); err != nil {
		cs.Logger.Error("[ProcessUnNameCheckRequest] (JsonData -> UnNameCheckDataFromQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessUnNameCheckRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	return cs.UnNameCheckService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, unNameCheckData, dataInQueue)
}

func (cs *CommonService) ProcessSbAccountCreationRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var response *common.IntegrationResponse
	var sbAccountData cif.CifEnquiryPayloadForAccountCreation
	var dataInQueue queue.DataPresentInQueue
	if err := json.Unmarshal(queueData.JsonData, &sbAccountData); err != nil {
		cs.Logger.Error("[ProcessSbAccountCreationRequest] (JsonData -> sbAccountData) ("+queueData.CustomerId+") ("+queueData.RequestName+") ", err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessSbAccountCreationRequest] (DataPresentInQueue)", "("+queueData.CustomerId+") ("+queueData.RequestName+")", err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	apiFlag, err := utils.CheckPrevAPIStatusInLifecycle(queueData.CustomerId, queueData.RequestName, queueData.ProductCode, queueData.DlqFlag)
	if err != nil {
		return nil, err
	}
	if !apiFlag {
		//retrigger the previous API
		return nil, config.GetErrorCode("DRBM0064", config.CommonErrorCodes)
	}
	return cs.SbAccCreationService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, sbAccountData, dataInQueue, queueData.RequestName)
}

func (cs *CommonService) ProcessSbAccountEnquiryRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var response *common.IntegrationResponse
	var dataInQueue queue.DataPresentInQueue
	var sbAccountEnquiryResponse sbAccountCreation.SBrespayload
	if err := json.Unmarshal(queueData.JsonData, &sbAccountEnquiryResponse); err != nil {
		cs.Logger.Error("[ProcessSbAccountEnquiryRequest] (JsonData) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessSbAccountEnquiryRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	return cs.SbEnquiryService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, dataInQueue, queueData.RequestName)
}

func (cs *CommonService) ProcessCollateralLodgeRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var resp *common.IntegrationResponse
	var dataInQueue queue.DataPresentInQueue
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessCollateralLodgeRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	apiFlag, err := utils.CheckPrevAPIStatusInLifecycle(queueData.CustomerId, queueData.RequestName, queueData.ProductCode, queueData.DlqFlag)
	if err != nil {
		return nil, err
	}
	if !apiFlag {
		//retrigger the previous API
		return nil, config.GetErrorCode("DRBM0064", config.CommonErrorCodes)
	}
	return cs.CollaterLodgeService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, dataInQueue)
}

func (cs *CommonService) ProcessCollateralLinkRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var response *common.IntegrationResponse
	var dataInQueue queue.DataPresentInQueue
	if err := json.Unmarshal(queueData.DataPresentInQueue, &dataInQueue); err != nil {
		cs.Logger.Error("[ProcessCollateralLinkRequest] (DataPresentInQueue) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
		return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	apiFlag, err := utils.CheckPrevAPIStatusInLifecycle(queueData.CustomerId, queueData.RequestName, queueData.ProductCode, queueData.DlqFlag)
	if err != nil {
		return nil, err
	}
	if !apiFlag {
		//retrigger the previous API
		return nil, config.GetErrorCode("DRBM0064", config.CommonErrorCodes)
	}
	return cs.CollateralLinkService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, dataInQueue)
}

func (cs *CommonService) GetRequestData(message []byte) (*common.GetRequestResponse, error) {
	var data queue.DataFromQueuePayload
	var response *common.GetRequestResponse
	var err error

	if err := json.Unmarshal(message, &data); err != nil {
		cs.Logger.Error("[GetRequestData] "+config.ErrorMessage, err)
		return nil, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	customerId := data.DataInQueue.Message.CustomerID
	requestName := data.DataInQueue.Message.RequestName
	callbackUrl := data.DataInQueue.Message.CallbackUrl
	referenceId := data.DataInQueue.Message.ReferenceId
	roleCode := data.DataInQueue.Message.RoleCode

	if requestName == "" || requestName == `""` {
		cs.Logger.Infof("[GetRequestData] " + "(" + customerId + ") (" + requestName + ") " + "End of graph flow. Can't traverse further")
		return nil, nil
	}

	jsonData := []byte(data.DataInQueue.Message.Response)
	dataPresentInQueue := []byte(data.DataInQueue.Message.DataPresentInQueue)

	if requestName == config.BankFinalityRequest {
		requestName, dataPresentInQueue, jsonData, err = cs.ProcessBankFinalityRequest(customerId, jsonData, dataPresentInQueue, data)
		if err != nil {
			return nil, err
		}
	}

	if requestName == config.TermLoanFinalityRequest {
		requestName, dataPresentInQueue, jsonData, err = cs.ProcessTermLoanBankFinalityRequest(customerId, jsonData, dataPresentInQueue, data)
		cs.Logger.Infof("[GetRequestData] (ProcessTermLoanBankFinalityRequest)" + "(" + customerId + ") next api to trigger:  (" + requestName + ") ")
		if err != nil {
			return nil, err
		}
	}

	if requestName == config.LrBankFlowRequest {
		requestName, dataPresentInQueue, jsonData, err = cs.ProcessLoanRenewalBankFlowRequest(customerId, jsonData, dataPresentInQueue, data)
		if err != nil {
			return nil, err
		}
	}

	if requestName == config.LrBankFinalityRequest {
		requestName, dataPresentInQueue, jsonData, err = cs.ProcessLrBankFinalityRequest(customerId, jsonData, dataPresentInQueue, data)
		if err != nil {
			return nil, err
		}
	}
	cs.Logger.Infof("[GetRequestData] " + "(" + customerId + ") (" + requestName + ") \n")
	cs.Logger.Debugf("[GetRequestData] (%s) (%s) Data from Queue: %v", customerId, requestName, data.DataInQueue.Message)

	response = &common.GetRequestResponse{
		RequestName: requestName, CustomerId: customerId, CallbackUrl: callbackUrl, ReferenceId: referenceId, RoleCode: roleCode, JsonData: jsonData, DataPresentInQueue: dataPresentInQueue, SchemeCode: data.DataInQueue.Message.SchemeCode, ProductCode: data.DataInQueue.Message.ProductCode, DlqFlag: data.DataInQueue.Message.DlqFlag}
	return response, nil
}

// ProcessBankFinalityRequest processes the BankFinalityRequest.
func (cs *CommonService) ProcessBankFinalityRequest(customerId string, jsonData, dataPresentInQueue []byte, data queue.DataFromQueuePayload) (string, []byte, []byte, error) {

	customerData, err := cs.CommonRepository.GetCustomerDataDetails(customerId, "Finality Flow")
	// requestName := data.DataInQueue.Message.RequestName
	cs.Logger.Info("The dedupe flag is: ", customerData.DedupeFlag)
	if err != nil {
		return "", nil, nil, err
	}

	if customerData.DedupeFlag == config.SuccessFlagInDB {
		jsonData = []byte(data.DataInQueue.Message.Response)
		var dataMap map[string]interface{}
		err := json.Unmarshal(jsonData, &dataMap)
		if err != nil {
			cs.Logger.Error("[ProcessBankFinalityRequest] "+"("+customerId+") "+config.ErrorMessage, err)
			return "", nil, nil, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
		}
		cifId, err := cs.CommonRepository.GetCifID(customerId, config.LoanCreationId)
		if err != nil {
			return "", nil, nil, err
		}

		///Sending Bypass to lifecycle
		cs.Logger.Info("Sending bypass status")
		if err := cs.CommonIntegrations.UpdateLifecycleBypassStatus(customerId, config.CifCreationId, config.BypassStatus, data.DataInQueue.Message.ProductCode); err != nil {
			return "", nil, nil, err
		}

		var processinstId string
		if _, ok := dataMap["ProcessinstId"]; ok {
			value, ok := dataMap["ProcessinstId"].(string)
			if ok {
				processinstId = value
			}
		}
		sign_Id, err := helpers.CreatecustomerSignPayload(dataMap)
		if err != nil {
			cs.Logger.Errorf("[ProcessBankFinalityRequest] Failed to create Sign_Id; error: %v", err)
		}
		queueData := queue.DataPresentInQueue{
			LoanAccountCustomerName:         dataMap["Personal_Details"].(map[string]interface{})["FullName"].(string),
			ProcessInstanceId:               processinstId,
			CIFID:                           cifId,
			TransactionID:                   dataMap["transaction_id"].(string),
			IFSC:                            dataMap["IFSC"].(string),
			ActivityCode:                    dataMap["ActivityCode"].(string),
			SanctionLimit:                   dataMap["SanctionLimit"].(string),
			LoanPeriodMonths:                dataMap["LoanPeriodMonths"].(string),
			InstallmentAmount:               dataMap["InstallmentAmount"].(string),
			InterestRate:                    dataMap["InterestRate"].(string),
			UtilisationDistrict:             dataMap["UtilisationDistrict"].(string),
			UtilisationState:                dataMap["UtilisationState"].(string),
			SavingsBankAccountNumber:        dataMap["SBAccNumber"].(string),
			SavingsBankCustomerName:         dataMap["SBCustomerName"].(string),
			SavingsBankCustomerMobileNumber: dataMap["SBCustomerMobileNo"].(string),
			PanNumber:                       dataMap["Identification_Details"].(map[string]interface{})["ProofOfIdentity"].(map[string]interface{})["Id_Number"].(string),
			Ekyc:                            dataMap["transaction_id"].(string),
			AssessedValue:                   dataMap["AssessedValue"].(string),
			DisbursementPreference:          dataMap["DisbursementPreference"].(string),
			BranchName:                      dataMap["BranchName"].(string),
			SolId:                           dataMap["SolId"].(string),
			NomineeName:                     dataMap["NomineeName"].(string),
			NomineeRelationship:             dataMap["NomineeRelationship"].(string),
			NomineeDOB:                      dataMap["NomineeDOB"].(string),
			SchemaCode:                      dataMap["SchemaCode"].(string),
			SectorCode:                      dataMap["SectorCode"].(string),
			ApiParam:                        dataMap["ApiParam"].(string),
			MCLRFixedSpread:                 dataMap["MCLRFixedSpread"].(string),
			SubSectorCode:                   dataMap["SubSectorCode"].(string),
			NetDisbursedAmount:              dataMap["NetDisbursedAmount"].(string),
			PurposeOfAdvance:                dataMap["PurposeOfAdvance"].(string),
			FreeCode9:                       dataMap["FreeCode9"].(string),
			Acres:                           dataMap["Acres"].(string),
			ClassificationCode:              dataMap["ClassificationCode"].(string),
			ClassificationOfBorrower:        dataMap["ClassificationOfBorrower"].(string),
			ProcessingFees:                  dataMap["ProcessingFees"].(string),
			CityCode:                        dataMap["Contact_Details"].(map[string]interface{})["Communication_Address"].(map[string]interface{})["City_Cd"].(string),
			StateCode:                       dataMap["Contact_Details"].(map[string]interface{})["Communication_Address"].(map[string]interface{})["State_Cd"].(string),
			SignId:                          sign_Id,
		}
		jsonBytes, err := json.Marshal(queueData)
		if err != nil {
			cs.Logger.Errorln("[ProcessBankFinalityRequest] "+"("+customerId+") "+config.ErrorMessage, "\nError: ", err.Error())
			return "", nil, nil, config.GetErrorCode(config.MarshallingDataErrorCode, config.CommonErrorCodes)
		}
		dataPresentInQueue := []byte(string(jsonBytes))
		jsonData = dataPresentInQueue
		if (customerData.SBAccountExists == config.SuccessFlagInDB && queueData.DisbursementPreference == config.DisbursementPreferenceOther) ||
			(customerData.SBAccountExists == config.FailureFlagInDB && queueData.DisbursementPreference == config.DisbursementPreferenceOther) ||
			(customerData.SBAccountExists == config.SuccessFlagInDB && queueData.DisbursementPreference == config.DisbursementPreferenceExisting) {
			cs.Logger.Info("Sending bypass status")
			if err := cs.CommonIntegrations.UpdateLifecycleBypassStatus(customerId, config.CifCreationId, config.BypassStatus, data.DataInQueue.Message.ProductCode); err != nil {
				return "", nil, nil, config.GetErrorCode("DRBM0065", config.CommonErrorCodes)
			}
			return config.LoanCreationId, dataPresentInQueue, jsonData, nil
		}
		return config.SbAccCreationId, dataPresentInQueue, jsonData, nil
	}
	return config.CifCreationId, dataPresentInQueue, jsonData, nil
}

func (cs *CommonService) ProcesssLrBorrowDetailsUpdateRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var resp *common.IntegrationResponse
	var lrQueuPayload lrBorrowDetails.RequestBorrowDetailsPayloadQueueData
	if err := json.Unmarshal([]byte(queueData.DataPresentInQueue), &lrQueuPayload); err != nil {
		cs.Logger.Error("[ProcesssLrBorrowDetailsUpdateRequest] (lrQueuPayload) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes), "\n Error: ", err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}

	var lrBankFlow loanRenewalFlow.RequestPayload

	if err := json.Unmarshal(queueData.DataPresentInQueue, &lrBankFlow); err != nil {
		cs.Logger.Error("[ProcesssLrBorrowDetailsUpdateRequest] (lrBankFlow) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes), "\n Error: ", err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}

	return cs.LrBorrowDetailsService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, queueData.RequestName, lrQueuPayload, queueData.DataPresentInQueue)
}

func (cs *CommonService) ProcessLrLatestInterestRateUpdRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var resp *common.IntegrationResponse
	var intrstRateData lrLatestInterestRate.RequestLatestIntrstRateQueueData

	if err := json.Unmarshal([]byte(queueData.DataPresentInQueue), &intrstRateData); err != nil {
		cs.Logger.Error("[ProcessLrLatestInterestRateUpdRequest] (lrQueuPayload) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes), "\n Error: ", err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}

	// previous api status
	prevApiStatus, err := cs.CommonRepository.GetCustomerStatusInCustomerLogs(queueData.CustomerId, config.LrBorrowDetailsUpd)
	if err != nil {
		cs.Logger.Error("[ProcessLrLatestInterestRateUpdRequest] (get previous api status) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, "\n Error: ", err)
		return resp, err
	}
	if !prevApiStatus {
		cs.Logger.Error("[ProcessLrLatestInterestRateUpdRequest] ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, " Previous Api is not executed: ", config.LrBorrowDetailsUpd)
		return resp, config.GetErrorCode("DRBM5005", config.CommonErrorCodes)
	}
	return cs.LrLatestInterestRateUpdateService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, queueData.RequestName, intrstRateData, queueData.DataPresentInQueue)
}

func (cs *CommonService) ProcessLrNextInterestDateUpdRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var resp *common.IntegrationResponse
	var lrQueuPayload lrNextInterestDate.RequestNextIntrstDateQueueData

	if err := json.Unmarshal([]byte(queueData.DataPresentInQueue), &lrQueuPayload); err != nil {
		cs.Logger.Error("[ProcessLrNextInterestDateUpdRequest] (lrQueuPayload) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes), "\n Error: ", err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	// previous api status
	prevApiStatus, err := cs.CommonRepository.GetCustomerStatusInCustomerLogs(queueData.CustomerId, config.LrLatestInterestRateUpd)
	if err != nil {
		cs.Logger.Error("[ProcessLrNextInterestDateUpdRequest] (get previous api status) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, "\n Error: ", err)
		return resp, err
	}
	if !prevApiStatus {
		cs.Logger.Error("[ProcessLrNextInterestDateUpdRequest] ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, " Previous Api is not executed: ", config.LrLatestInterestRateUpd)
		return resp, config.GetErrorCode("DRBM5005", config.CommonErrorCodes)
	}
	return cs.LrNextInterestDateUpdateService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, queueData.RequestName, lrQueuPayload, queueData.DataPresentInQueue)
}

func (cs *CommonService) ProcessLrSanctionLimitAndDtlsUpdRequst(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	var resp *common.IntegrationResponse
	var lrQueuPayload lrSanctionLimitAndDetailsUpd.RequestSanctionLimitQueueData

	if err := json.Unmarshal([]byte(queueData.DataPresentInQueue), &lrQueuPayload); err != nil {
		cs.Logger.Error("[ProcessLrSanctionLimitAndDtlsUpdRequst] (lrQueuPayload) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes), "\n Error: ", err)
		return resp, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}

	// previous api status
	prevApiStatus, err := cs.CommonRepository.GetCustomerStatusInCustomerLogs(queueData.CustomerId, config.LrNextInterestDateUpd)
	if err != nil {
		cs.Logger.Error("[ProcessLrSanctionLimitAndDtlsUpdRequst] (get previous api status) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, "\n Error: ", err)
		return resp, err
	}
	if !prevApiStatus {
		cs.Logger.Error("[ProcessLrSanctionLimitAndDtlsUpdRequst] ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, " Previous Api is not executed: ", config.LrLatestInterestRateUpd)
		return resp, config.GetErrorCode("DRBM5005", config.CommonErrorCodes)
	}

	return cs.lrSanctionLimitAndDetailsUpdateService(queueData.CustomerId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, queueData.RequestName, lrQueuPayload, queueData.DataPresentInQueue)
}

func (cs *CommonService) ProcessLrBankFinalityRequest(customerId string, jsonData, dataPresentInQueue []byte, data queue.DataFromQueuePayload) (string, []byte, []byte, error) {
	// previous api status
	prevApiStatus, err := cs.CommonRepository.GetCustomerStatusInCustomerLogs(customerId, config.LrSanctionLimitAndDtlsUpd)
	if err != nil {
		cs.Logger.Error("[ProcessLrBankFinalityRequest] (get previous api status) ("+customerId+") "+config.ErrorMessage, "\n Error: ", err)
		return "", nil, nil, err
	}
	if !prevApiStatus {
		cs.Logger.Error("[ProcessLrBankFinalityRequest] ("+customerId+") "+config.ErrorMessage, " Previous Api is not executed: ", config.LrSanctionLimitAndDtlsUpd)
		return "", nil, nil, config.GetErrorCode("DRBM5005", config.CommonErrorCodes)
	}

	customerData, err := cs.CommonRepository.GetCustomerDataDetails(customerId, "LR Finality Flow")
	if err != nil {
		return "", nil, nil, err
	}
	cs.Logger.Info("The dedupe flag is: ", customerData.DedupeFlag)
	if customerData.DedupeFlag == config.SuccessFlagInDB {
		var dataMap map[string]interface{}
		err := json.Unmarshal(jsonData, &dataMap)
		if err != nil {
			cs.Logger.Error("[ProcessLrBankFinalityRequest] "+"("+customerId+") "+config.ErrorMessage, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes), "\nError: ", err)
			return "", nil, nil, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
		}

		///Sending Bypass to lifecycle
		cs.Logger.Info("Sending bypass status")
		if err := cs.CommonIntegrations.UpdateLifecycleBypassStatus(customerId, config.CifCreationId, config.BypassStatus, data.DataInQueue.Message.ProductCode); err != nil {
			return "", nil, nil, err
		}
		var queueData queue.DataPresentInQueue
		if err := json.Unmarshal(dataPresentInQueue, &queueData); err != nil {
			cs.Logger.Errorln("[ProcessLrBankFinalityRequest] "+"("+customerId+") "+config.ErrorMessage, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes), "\nError: ", err)
			return "", nil, nil, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
		}
		jsonBytes, err := json.Marshal(queueData)
		if err != nil {
			cs.Logger.Errorln("[ProcessLrBankFinalityRequest] "+"("+customerId+") "+config.ErrorMessage, config.GetErrorCode(config.MarshallingDataErrorCode, config.CommonErrorCodes), "\nError: ", err)
			return "", nil, nil, config.GetErrorCode(config.MarshallingDataErrorCode, config.CommonErrorCodes)
		}
		dataPresentInQueue := []byte(string(jsonBytes))
		jsonData = dataPresentInQueue

		if (customerData.SBAccountExists == config.SuccessFlagInDB && queueData.DisbursementPreference == config.DisbursementPreferenceOther) ||
			(customerData.SBAccountExists == config.FailureFlagInDB && queueData.DisbursementPreference == config.DisbursementPreferenceOther) ||
			(customerData.SBAccountExists == config.SuccessFlagInDB && queueData.DisbursementPreference == config.DisbursementPreferenceExisting) {
			cs.Logger.Info("Sending bypass status")
			if err := cs.CommonIntegrations.UpdateLifecycleBypassStatus(customerId, config.CifCreationId, config.BypassStatus, data.DataInQueue.Message.ProductCode); err != nil {
				return "", nil, nil, config.GetErrorCode("DRBM0065", config.CommonErrorCodes)
			}
			return config.LrNEFTFundTransferId, dataPresentInQueue, jsonData, nil
		}
		// only hit sbCreation if new to bank other wise
		return config.LrSbAccCreationId, dataPresentInQueue, jsonData, nil
	}
	return "", nil, nil, config.GetErrorCode("DRBM5004", config.CommonErrorCodes)
}

func (cs *CommonService) ProcessLoanRenewalBankFlowRequest(customerId string, jsonData, dataPresentInQueue []byte, data queue.DataFromQueuePayload) (string, []byte, []byte, error) {
	requestName := data.DataInQueue.Message.RequestName
	var lrQueuPayload loanRenewalFlow.LoanRenewalQueuePayload
	if err := json.Unmarshal(jsonData, &lrQueuPayload); err != nil {
		cs.Logger.Error("[ProcessLoanRenewalBankFlowRequest] "+"("+customerId+") "+config.ErrorMessage, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes), "\nError: ", err)
		return "", nil, nil, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	queueData := queue.DataPresentInQueue{
		LoanAccountCustomerName:         lrQueuPayload.PersonalDetails.FullName,
		ProcessInstanceId:               lrQueuPayload.ProcessInstanceId,
		TransactionID:                   lrQueuPayload.TransactionID,
		IFSC:                            lrQueuPayload.IFSC,
		ActivityCode:                    lrQueuPayload.ActivityCode,
		SanctionLimit:                   lrQueuPayload.LoanRenewalData.SanctionLimit,
		LoanPeriodMonths:                lrQueuPayload.LoanPeriodMonths,
		InstallmentAmount:               lrQueuPayload.InstallmentAmount,
		InterestRate:                    lrQueuPayload.CustPrefIntDr,
		UtilisationDistrict:             lrQueuPayload.UtilisationDistrict,
		UtilisationState:                lrQueuPayload.UtilisationState,
		SavingsBankAccountNumber:        lrQueuPayload.SavingsBankAccountNumber,
		SavingsBankCustomerName:         lrQueuPayload.SavingsBankCustomerName,
		SavingsBankCustomerMobileNumber: lrQueuPayload.SavingsBankCustomerMobileNumber,
		PanNumber:                       lrQueuPayload.IdentificationDetails.ProofOfIdentity.IdNumber,
		Ekyc:                            lrQueuPayload.TransactionID,
		AssessedValue:                   lrQueuPayload.AssessedValue,
		DisbursementPreference:          lrQueuPayload.DisbursementPreference,
		BranchName:                      lrQueuPayload.BranchName,
		SolId:                           lrQueuPayload.SolId,
		NomineeName:                     lrQueuPayload.NomineeName,
		NomineeRelationship:             lrQueuPayload.NomineeRelationship,
		NomineeDOB:                      lrQueuPayload.NomineeDOB,
		SchemaCode:                      lrQueuPayload.SchemaCode,
		SectorCode:                      lrQueuPayload.Sector,
		ApiParam:                        lrQueuPayload.TableCode,
		MCLRFixedSpread:                 lrQueuPayload.MCLRFixedSpread,
		SubSectorCode:                   lrQueuPayload.SubSector,
		NetDisbursedAmount:              lrQueuPayload.NetDisbursedAmount,
		PurposeOfAdvance:                lrQueuPayload.AdvancePurpose,
		FreeCode9:                       lrQueuPayload.FreeCode9,
		Acres:                           lrQueuPayload.Acres,
		ClassificationCode:              lrQueuPayload.Clasification,
		ClassificationOfBorrower:        lrQueuPayload.ClasificationBorrower,
		ProcessingFees:                  lrQueuPayload.ProcessingFees,
		CityCode:                        lrQueuPayload.ContactDetails.CommunicationAddress.CityCd,
		StateCode:                       lrQueuPayload.ContactDetails.CommunicationAddress.StateCd,
		DocDate:                         lrQueuPayload.LoanRenewalData.DocDate,
		UnFreezeSubvention:              lrQueuPayload.LoanRenewalData.UnFreezeSubvention,
		Category:                        lrQueuPayload.LoanRenewalData.Category,
		AppFromDate:                     lrQueuPayload.LoanRenewalData.AppFromDate,
		AccountNumber:                   lrQueuPayload.LoanRenewalData.AccountNumber,
		LandValue:                       lrQueuPayload.LoanRenewalData.LandValue,
		CIFID:                           lrQueuPayload.LoanRenewalData.CIFID,
		AccOpnDate:                      lrQueuPayload.LoanRenewalData.AccOpnDate,
	}
	dataPresentInQueue, err := json.Marshal(queueData)
	if err != nil {
		cs.Logger.Errorln("[ProcessBankFinalityRequest] "+"("+customerId+") "+config.ErrorMessage, config.GetErrorCode(config.MarshallingDataErrorCode, config.CommonErrorCodes), "\nError: ", err)
		return "", nil, nil, config.GetErrorCode(config.MarshallingDataErrorCode, config.CommonErrorCodes)
	}
	var lrBankFlow loanRenewalFlow.RequestPayload
	if err := json.Unmarshal(dataPresentInQueue, &lrBankFlow); err != nil {
		cs.Logger.Error("[ProcessLoanRenewalBankFlowRequest] (lrBankFlow) ("+customerId+") ("+requestName+") "+config.ErrorMessage, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes), "\n Error: ", err)
		return "", nil, nil, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}
	return config.LrBorrowDetailsUpd, dataPresentInQueue, jsonData, nil
}

func (cs *CommonService) ProcessInternalLrBankFinalityRequest(queueReqRes *common.GetRequestResponse) (*common.IntegrationResponse, error) {
	customerId := queueReqRes.CustomerId
	requestName := queueReqRes.RequestName
	cs.Logger.Infof("[ProcessInternalLrBankFinalityRequest] customerId: %s, requestName: %s starts with DataPresentInQueue: %s\n", customerId, requestName, string(queueReqRes.DataPresentInQueue))
	customerData, err := cs.CommonRepository.GetCustomerDataDetails(customerId, "LR Internal Finality Flow")
	cs.Logger.Info("The dedupe flag is: ", customerData.DedupeFlag)
	if err != nil {
		return nil, err
	}
	if customerData.DedupeFlag == config.SuccessFlagInDB {
		var queueData queue.DataPresentInQueue
		err := json.Unmarshal(queueReqRes.DataPresentInQueue, &queueData)
		if err != nil {
			cs.Logger.Error("[ProcessInternalLrBankFinalityRequest] "+"("+customerId+") "+config.ErrorMessage, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes), "\nError: ", err)
			return nil, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
		}
		cs.Logger.Info("Sending bypass status")
		if err := cs.CommonIntegrations.UpdateLifecycleBypassStatus(customerId, config.CifCreationId, config.BypassStatus, queueReqRes.ProductCode); err != nil {
			return nil, err
		}

		if (customerData.SBAccountExists == config.SuccessFlagInDB && queueData.DisbursementPreference == config.DisbursementPreferenceOther) ||
			(customerData.SBAccountExists == config.FailureFlagInDB && queueData.DisbursementPreference == config.DisbursementPreferenceOther) ||
			(customerData.SBAccountExists == config.SuccessFlagInDB && queueData.DisbursementPreference == config.DisbursementPreferenceExisting) {
			cs.Logger.Info("Sending bypass status")
			if err := cs.CommonIntegrations.UpdateLifecycleBypassStatus(customerId, requestName, config.BypassStatus, queueReqRes.ProductCode); err != nil {
				return nil, config.GetErrorCode("DRBM0065", config.CommonErrorCodes)
			}

			queueReqRes.RequestName = config.LrNEFTFundTransferId
			return cs.ProcessNEFTFundTransferRequest(queueReqRes)
		}
		queueReqRes.RequestName = config.LrSbAccCreationId
		return cs.ProcessSbAccountCreationRequest(queueReqRes)
	}

	return nil, config.GetErrorCode("DRBM5004", config.CommonErrorCodes)
}

func (cs *CommonService) ProcessTermLoanBankFinalityRequestInitial(customerId string, jsonData, dataPresentInQueue []byte, data queue.DataFromQueuePayload) (string, []byte, []byte, error) {

	customerData, err := cs.CommonRepository.GetCustomerDataDetails(customerId, "Term Loan Finality Flow")
	// requestName := data.DataInQueue.Message.RequestName
	cs.Logger.Info("The dedupe flag is: ", customerData.DedupeFlag)
	if err != nil {
		return "", nil, nil, err
	}

	jsonData = []byte(data.DataInQueue.Message.Response)
	var dataMap map[string]interface{}
	err = json.Unmarshal(jsonData, &dataMap)
	if err != nil {
		cs.Logger.Error("[ProcessTermLoanBankFinalityRequest] "+"("+customerId+") "+config.ErrorMessage, err)
		return "", nil, nil, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
	}

	cifId, err := cs.CommonRepository.GetCifID(customerId, config.TermLoanFinalityRequest)
	if err != nil {
		return "", nil, nil, err
	}
	///Sending Bypass to lifecycle
	cs.Logger.Info("Sending bypass status")
	// if err := cs.CommonIntegrations.UpdateLifecycleBypassStatus(customerId, config.CifCreationId, config.BypassStatus, data.DataInQueue.Message.ProductCode); err != nil {
	// 	return "", nil, nil, err
	// }

	dataPresentInQueue = []byte(string(jsonData))
	jsonData = dataPresentInQueue
	// return config.CoCifCreationId, dataPresentInQueue, jsonData, nil
	if customerData.DedupeFlag == config.SuccessFlagInDB {

		if len(cifId) == 0 {
			return "", nil, nil, fmt.Errorf("%s", "dedupe flag is Y but cifId not saved in db.")
		}
		// TOOD:check if co-applicant dedupe is y then trigger applicant loan acccount creation
		return config.CoCifCreationId, dataPresentInQueue, jsonData, nil
	}
	return config.CifCreationId, dataPresentInQueue, jsonData, nil
}

func (cs *CommonService) ProcessTermLoanBankFinalityRequest(customerId string, jsonData, dataPresentInQueue []byte, data queue.DataFromQueuePayload) (string, []byte, []byte, error) {

	customerData, err := cs.CommonRepository.GetCustomerDataDetails(customerId, "Term Loan Finality Flow")
	cs.Logger.Info("The dedupe flag is: ", customerData.DedupeFlag)
	if err != nil {
		return "", nil, nil, err
	}

	if customerData.DedupeFlag == config.SuccessFlagInDB {
		jsonData = []byte(data.DataInQueue.Message.Response)
		var dataMap queue.TermLoanQueueData
		// map[string]interface{}
		err := json.Unmarshal(jsonData, &dataMap)
		if err != nil {
			cs.Logger.Error("[ProcessTermLoanBankFinalityRequest] "+"("+customerId+") "+config.ErrorMessage, err)
			return "", nil, nil, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
		}

		cs.Logger.Infof("custoemr (%s) has CifId  (%s)", customerId, customerData.CifID)

		if len(customerData.CifID) == 0 {
			return "", nil, nil, fmt.Errorf("%s", "dedupe flag is Y but cifId not saved in db.")
		}
		///Sending Bypass to lifecycle
		cs.Logger.Info("Sending bypass status")
		if err := cs.CommonIntegrations.UpdateLifecycleBypassStatus(customerId, config.CifCreationId, config.BypassStatus, data.DataInQueue.Message.ProductCode); err != nil {
			return "", nil, nil, err
		}

		queueData := queue.DataPresentInQueue{
			LoanAccountCustomerName:         dataMap.PersonalDetails.FullName,
			CIFID:                           customerData.CifID,
			TransactionID:                   dataMap.TransactionID,
			IFSC:                            dataMap.IFSC,
			ActivityCode:                    dataMap.ActivityCode,
			SanctionLimit:                   dataMap.SanctionLimit,
			LoanPeriodMonths:                dataMap.LoanPeriodMonths,
			InstallmentAmount:               dataMap.InstallmentAmount,
			InterestRate:                    dataMap.InterestRate,
			UtilisationDistrict:             dataMap.UtilisationDistrict,
			UtilisationState:                dataMap.UtilisationState,
			SavingsBankAccountNumber:        dataMap.SavingsBankAccountNumber,
			SavingsBankCustomerName:         dataMap.SavingsBankCustomerName,
			SavingsBankCustomerMobileNumber: dataMap.SavingsBankCustomerMobileNumber,
			PanNumber:                       dataMap.IdentificationDetails.ProofOfIdentity.IDNumber,
			Ekyc:                            dataMap.TransactionID,
			AssessedValue:                   dataMap.AssessedValue,
			DisbursementPreference:          dataMap.DisbursementPreference,
			BranchName:                      dataMap.BranchName,
			SolId:                           dataMap.SolId,
			NomineeName:                     dataMap.NomineeName,
			NomineeRelationship:             dataMap.NomineeRelationship,
			NomineeDOB:                      dataMap.NomineeDOB,
			SchemaCode:                      dataMap.SchemaCode,
			SectorCode:                      dataMap.SectorCode,
			ApiParam:                        dataMap.ApiParam,
			MCLRFixedSpread:                 dataMap.MCLRFixedSpread,
			SubSectorCode:                   dataMap.SubSectorCode,
			NetDisbursedAmount:              dataMap.NetDisbursedAmount,
			PurposeOfAdvance:                dataMap.PurposeOfAdvance,
			FreeCode9:                       dataMap.FreeCode9,
			Acres:                           dataMap.Acres,
			ClassificationCode:              dataMap.ClassificationCode,
			ClassificationOfBorrower:        dataMap.ClassificationOfBorrower,
			ProcessingFees:                  dataMap.ProcessingFees,
			CityCode:                        dataMap.ContactDetails.CommunicationAddress.CityCode,
			StateCode:                       dataMap.ContactDetails.CommunicationAddress.StateCode,
			SignId:                          dataMap.SignId,
			ApplicantId:                     customerId,
			CoApplicantData:                 dataMap.CoApplicantData,
			InterestSubventionFlag:          dataMap.InterestSubventionFlag,
			InstallamentAmount:              dataMap.InstallamentAmount,
			NoOfInstallment:                 dataMap.NoOfInstallment,
			FirstEmiDate:                    dataMap.FirstEmiDate,
		}
		jsonBytes, err := json.Marshal(queueData)
		if err != nil {
			cs.Logger.Errorln("[ProcessTermLoanBankFinalityRequest] "+"("+customerId+") "+config.ErrorMessage, "\nError: ", err.Error())
			return "", nil, nil, config.GetErrorCode(config.MarshallingDataErrorCode, config.CommonErrorCodes)
		}
		dataPresentInQueue := []byte(string(jsonBytes))

		coAppId, err := cs.CommonRepository.GetCoApplicantId(customerId, data.DataInQueue.Message.ProductCode)
		if err != nil {
			cs.Logger.Error("[ProcessCoApplicantCifCreationRequest] (GetCoApplicantId)"+" for custoemr: ("+customerId+") "+config.ErrorMessage, err)
			return "", nil, nil, err
		}

		if len(coAppId) == 0 {
			cs.Logger.Error("[ProcessCoApplicantCifCreationRequest] (" + customerId + ")  is missing co applicant")
			return "", nil, nil, fmt.Errorf("co-applicant Not found for customer (%s)", customerId)
		}

		CoApplicantData, err := cs.CommonRepository.GetCustomerDataDetails(coAppId, "Term Loan Finality Flow")
		cs.Logger.Info("The dedupe flag of co-applicant is: ", CoApplicantData.DedupeFlag)
		if err != nil {
			return "", nil, nil, err
		}

		if CoApplicantData.DedupeFlag == config.SuccessFlagInDB {
			jsonData = dataPresentInQueue
			return config.LoanCreationId, dataPresentInQueue, jsonData, nil
		}
		return config.CoCifCreationId, dataPresentInQueue, jsonData, nil
	}
	return config.CifCreationId, dataPresentInQueue, jsonData, nil
}

func (cs *CommonService) ProcessCoApplicantCifCreationRequest(queueData *common.GetRequestResponse) (*common.IntegrationResponse, error) {

	requestDelayStr := os.Getenv("REQUEST_DELAY")
	requestDelay, _ := strconv.Atoi(requestDelayStr)
	time.Sleep(time.Duration(requestDelay) * time.Second)
	//TOOD: check applicant cif
	apiData, err := cs.CommonRepository.CheckApiStatus(queueData.CustomerId, queueData.ProductCode, config.LifecycleLevelMapper["cif_creation"])
	if err != nil {
		return nil, err
	}

	if apiData != config.SuccessStatus && apiData != config.BypassStatus {
		cs.Logger.Error("[ProcessCoApplicantCifCreationRequest] applicant api status: " + apiData)
		return nil, fmt.Errorf("CIF API status is not success for applicant: %s, requestName: %s", queueData.CustomerId, queueData.RequestName)
	}

	coAppId, err := cs.CommonRepository.GetCoApplicantId(queueData.CustomerId, queueData.ProductCode)
	if err != nil {
		cs.Logger.Error("[ProcessCoApplicantCifCreationRequest] (GetCoApplicantId)"+" for custoemr: ("+queueData.CustomerId+") "+config.ErrorMessage, err)
		return nil, err
	}

	if len(coAppId) == 0 {
		cs.Logger.Error("[ProcessCoApplicantCifCreationRequest] (" + queueData.CustomerId + ")  is missing co applicant")
		return nil, fmt.Errorf("o-applicant Not found for customer (%s)", queueData.CustomerId)
	}

	customerData, err := cs.CommonRepository.GetCustomerDataDetails(coAppId, config.CoCifCreationId)
	if err != nil {
		cs.Logger.Error("[ProcessCoApplicantCifCreationRequest] (GetCustomerDataDetails) ("+queueData.CustomerId+")", err)
		return nil, err
	}
	cs.Logger.Info("The dedupe flag is: ", customerData.DedupeFlag)

	if customerData.DedupeFlag != config.SuccessFlagInDB {
		var dataMap map[string]interface{}
		err := json.Unmarshal(queueData.DataPresentInQueue, &dataMap)
		if err != nil {
			cs.Logger.Error("[ProcessCoApplicantCifCreationRequest] "+"("+coAppId+") "+config.ErrorMessage, err)
			return nil, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
		}
		marshedJson, err := json.Marshal(dataMap["CoApplicantData"])
		if err != nil {
			cs.Logger.Error("[ProcessCoApplicantCifCreationRequest] "+"("+coAppId+") "+config.ErrorMessage, err)
			return nil, config.GetErrorCode(config.MarshallingDataErrorCode, config.CommonErrorCodes)
		}

		var response *common.IntegrationResponse
		var cifData cif.CifPayloadData
		if err := json.Unmarshal(marshedJson, &cifData); err != nil {
			cs.Logger.Error("[ProcessCoApplicantCifCreationRequest] (marshedJson) ("+coAppId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
			return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
		}

		var dataPresentInQueue queue.DataPresentInQueue
		if err := json.Unmarshal(queueData.DataPresentInQueue, &dataPresentInQueue); err != nil {
			cs.Logger.Error("[ProcessCoApplicantCifCreationRequest] (JsonData) ("+queueData.CustomerId+") ("+queueData.RequestName+") "+config.ErrorMessage, err)
			return response, config.GetErrorCode(config.UnmarshallingDataErrorCode, config.CommonErrorCodes)
		}

		apiFlag, err := utils.CheckPrevAPIStatusInLifecycle(coAppId, config.CifCreationId, queueData.ProductCode, queueData.DlqFlag)
		if err != nil {
			return nil, err
		}
		if !apiFlag {
			//retrigger the previous API
			return nil, config.GetErrorCode("DRBM0064", config.CommonErrorCodes)
		}

		cifData.SolId = dataPresentInQueue.SolId
		return cs.CifCreationService(coAppId, queueData.ProductCode, queueData.SchemeCode, queueData.CallbackUrl, queueData.ReferenceId, cifData, config.CoCifCreationId, dataPresentInQueue)

	}
	return cs.ProcessLoanAccountCreationRequest(queueData)
}
