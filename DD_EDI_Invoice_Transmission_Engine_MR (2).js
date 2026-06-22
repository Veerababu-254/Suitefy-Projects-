/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/runtime', 'N/search', 'N/url', "N/redirect", "N/log", "N/task", 'N/record', 'N/file', 'N/format',
        './DoorDashTool.js', "N/encode", "./DD_Constant"],

    (runtime, search, url, redirect, log, task, record, file, format, ddt,
     encode, DD_CONSTANT) => {
        'use strict';


        /**
         * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
         * @param {Object} inputContext
         * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Object} inputContext.ObjectRef - Object that references the input data
         * @typedef {Object} ObjectRef
         * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
         * @property {string} ObjectRef.type - Type of the record instance that contains the input data
         * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
         * @since 2015.2
         */

        const getInputData = (inputContext) => {
            try {
                const configureInfo = ddt.getDoorDashConfigInfo();
                let reprocessFileInfo = ddt.getPermanentSkippedFiles(configureInfo);
                reprocessFileInfo = ddt.getForcePostFileInfo(reprocessFileInfo);
                let fileArr = configureInfo.cabinetId ? ddt.getNew810InvoiceFiles(configureInfo.cabinetId, configureInfo.retryCabinetId, reprocessFileInfo) : [];
                let invoiceInfoArr = [];
                const fileIdInfo = {};
                const historicalInvoiceNumInfo = {};
                let invoiceNumInfo = {};
                let exceedUsage = ddt.getInvoicesInfo(fileArr, invoiceInfoArr, configureInfo, fileIdInfo,
                    historicalInvoiceNumInfo, invoiceNumInfo);
                if (exceedUsage) {
                    log.error("SSS_USAGE_LIMIT_EXCEEDED", "Please run again after script completed this time");
                }
                log.debug("invoiceInfoArr.length", invoiceInfoArr.length);
                return invoiceInfoArr;
            } catch (e) {
                log.error("Exception on getInputData", e);
                return [];
            }
        }

        /**
         * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
         * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
         * context.
         * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
         *     is provided automatically based on the results of the getInputData stage.
         * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
         *     function on the current key-value pair
         * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
         *     pair
         * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} mapContext.key - Key to be processed during the map stage
         * @param {string} mapContext.value - Value to be processed during the map stage
         * @since 2015.2
         */

        const map = (mapContext) => {
            let processedHistoryRecId = "";
            let processStatusRecInfo = "";
            let fileId = "";
            const startTime = new Date().getTime();
            try {
                // Usage limit 1000
                const invoiceInfo = mapContext.value && JSON.parse(mapContext.value);
                if (!invoiceInfo){
                    return "";
                }
                const vendorInfo = ddt.getVendorInfo(invoiceInfo.vendorId);
                fileId = invoiceInfo.fileId;
                const senderId = invoiceInfo.senderId;
                const separator = invoiceInfo.separator;
                const invoiceContent = invoiceInfo.invoice;
                const doordashConfigureInfo = invoiceInfo.configureInfo;
                // let fileName = invoiceInfo.fileName;
              log.audit({title:"File details Inv Num : ",details:invoiceInfo.invoiceNum})
              log.audit({title:"File ddt.comeFromFinTech(senderId) : ",details:ddt.comeFromFinTech(senderId)})
                let fileOriginalName = ddt.conversionFileName(invoiceInfo.fromFileName, vendorInfo.name, ddt.comeFromFinTech(senderId) ? invoiceInfo.invoiceNum : "");
                const matchedFileInfo = ddt.getMatchedFileRecordInfo(fileOriginalName, ddt.comeFromFinTech(senderId) ? invoiceInfo.invoiceNum : "", senderId);
                const successInvoiceInfo = matchedFileInfo && matchedFileInfo.successInvoiceInfo && JSON.parse(matchedFileInfo.successInvoiceInfo);
                if (ddt.comeFromFinTech(senderId) && invoiceInfo.multipleInvoices) {
                    if (invoiceInfo.firstInvoice) {
                        // move original fintech file to success file, so it can't be processed again at next time
                        const finTechFile = file.load({id: invoiceInfo.fromFileId});
                        if (finTechFile) {
                            const matchedFile = ddt.getMatchedFileInfo(invoiceInfo.successCabinetId, finTechFile.name);
                            if (matchedFile) {
                                log.error("delete old file", matchedFile);
                                file.delete({id: matchedFile.id});
                            }
                            finTechFile.folder = invoiceInfo.successCabinetId;
                            finTechFile.save();
                        }
                    }
                    // create a file for each invoice which come from FinTech.
                    const newInvoiceFileId = ddt.createFileForFinTech(fileOriginalName, invoiceInfo.header + separator + invoiceContent + invoiceInfo.footer);
                    if (newInvoiceFileId) {
                        // route to process current invoice file
                        fileId = newInvoiceFileId;
                        invoiceInfo.fileId = newInvoiceFileId;
                        invoiceInfo.fileName = fileOriginalName;
                    }
                }
                const matchedProcessedHistoryRecInfo = ddt.getMatchedProcessedHistoryRecInfo(fileOriginalName, invoiceInfo.invoiceNum, senderId);
                const forceToBillDays = invoiceInfo.days;
                const duplicateInvoice = invoiceInfo.duplicateInvoice;
                const duplicateHandled = invoiceInfo.duplicateHandled;
                const newFile = invoiceInfo.newFile;
                const invoiceEdiFile = fileId && file.load({id: fileId});
                const daysBeforeStop = doordashConfigureInfo && doordashConfigureInfo.daysBeforeStop;
                // File date created
                const fileCreated = invoiceInfo.fileCreated && new Date(invoiceInfo.fileCreated);
                processStatusRecInfo = {
                    name: "",
                    fileId: fileId,
                    fileName: fileOriginalName,
                    fromFileId: invoiceInfo.fromFileId,
                    fromFileName: invoiceInfo.fromFileName,
                    retryCabinetId: invoiceInfo.retryCabinetId,
                    successCabinetId: invoiceInfo.successCabinetId,
                    //Success
                    processStatus: DD_CONSTANT.HISTORY_PROCESS_STATUS.SUCCESS,
                    failReason: "",
                    count: 1,
                    firstInvoice: invoiceInfo.firstInvoice,
                    invoiceNumber: invoiceInfo.invoiceNum,
                    invoiceDateString: invoiceInfo.invoiceDateString,
                    duplicateInvoice: duplicateInvoice,
                    duplicateHandled: duplicateHandled,
                    newFile: newFile,
                    poNumber: "",
                    poId: "",
                    buyerEmail: "",
                    vendorId: invoiceInfo.vendorId,
                    typeCode: "",
                    vendorBillId: "",
                    vendorBillIdArr: [],
                    vendorBillIdInfo: {},
                    errorCode: "",
                    doordashConfigureInfo: doordashConfigureInfo,
                    senderId: senderId,
                    comeFromSPS: ddt.comeFromSPS(senderId),
                    comeFromFinTech: ddt.comeFromFinTech(senderId),
                    matchedFileInfo: matchedFileInfo,
                    matchedProcessedHistoryRecInfo: matchedProcessedHistoryRecInfo,
                    vendorInfo: vendorInfo,
                    reprocess: invoiceInfo.reprocess,
                    sourceKeyInfo: {
                        vendorCodeInfo: {},
                        vendorCodeArr: [],
                        upcInfo: {},
                        upcArr: [],
                        skuInfo: {},
                        skuArr: []
                    },
                    warning: ""
                };
                let saveProcessHistoryRecord;
                if (invoiceInfo.vendorId && (!vendorInfo || !vendorInfo.id)) {
                    //Failure
                    processStatusRecInfo.processStatus = DD_CONSTANT.HISTORY_PROCESS_STATUS.FAILURE;
                    processStatusRecInfo.failReason = `Invalid Vendor Id (There is no corresponding vendor for id: ${invoiceInfo.vendorId})`;
                    processStatusRecInfo.vendorId = "";
                    saveProcessHistoryRecord = true;
                }
                try {
                    if (processStatusRecInfo.processStatus != DD_CONSTANT.HISTORY_PROCESS_STATUS.FAILURE) {
                        saveProcessHistoryRecord = ddt.mainProcessForInvoice({
                            invoiceInfo: invoiceInfo,
                            fileId: fileId,
                            fileOriginalName: fileOriginalName,
                            successInvoiceInfo: successInvoiceInfo,
                            forceToBillDays: forceToBillDays,
                            separator: separator,
                            senderId: senderId,
                            invoiceContent: invoiceContent,
                            invoiceEdiFile: invoiceEdiFile,
                            doordashConfigureInfo: doordashConfigureInfo,
                            daysBeforeStop: daysBeforeStop,
                            // File date created
                            fileCreated: fileCreated,
                            processStatusRecInfo: processStatusRecInfo,
                            duplicateInvoice: duplicateInvoice,
                            duplicateHandled: duplicateHandled,
                            newFile: newFile
                        });
                    }
                } catch (e) {
                    //Failure
                    processStatusRecInfo.processStatus = DD_CONSTANT.HISTORY_PROCESS_STATUS.FAILURE;
                    processStatusRecInfo.failReason = "Failed to process, " + e.message;
                    saveProcessHistoryRecord = true;
                }
                if (saveProcessHistoryRecord) {
                    try {
                        // Update to append vendor name to invoice file and format file type with txt
                        processStatusRecInfo.fileNameChanged = ddt.conversionForFile(invoiceEdiFile, processStatusRecInfo.vendorName);
                        processStatusRecInfo.fileName = invoiceEdiFile.name;
                        // Save processed info as history record.
                        if (processStatusRecInfo.processStatus == DD_CONSTANT.HISTORY_PROCESS_STATUS.FAILURE && !processStatusRecInfo.errorCode) {
                            processStatusRecInfo.errorCode = DD_CONSTANT.ERROR_CODE.DEFAULT;
                        }
                        processedHistoryRecId = ddt.saveProcessedInfo({
                            invoiceEdiFile: invoiceEdiFile,
                            daysBeforeStop: daysBeforeStop,
                            // File date created
                            fileCreated: fileCreated,
                            processStatusRecInfo: processStatusRecInfo
                        });
                        if (processStatusRecInfo.reprocess) {
                            try{ 
                              // Updated based on FINAPPS-11058
                                //processStatusRecInfo.vendorId && record.submitFields({
                                  //  type: record.Type.VENDOR,
                                   // id: processStatusRecInfo.vendorId,
                                   // values: {
                                        //custentity_dd_edi_reprocess_back_x_days: ""
                                   // },
                                   // options: {
                                       // ignoreMandatoryFields: true
                                   // }
                               // });
                            }catch (err) {
                                log.error("Field id: " + fileId + " Clear Back X Days Part1", err);
                            }
                        }
                    } catch (e) {
                        log.error("Field id: " + fileId + " Part2: Exception on save process status record", e);
                    }
                } else {
                    try {
                       // record.submitFields({
                          //  type: record.Type.VENDOR,
                           // id: processStatusRecInfo.vendorId,
                           // values: {
                               // custentity_dd_edi_reprocess_back_x_days: ""
                            //},
                            //options: {
                                //ignoreMandatoryFields: true
                           // }
                       // });
                    }catch (e) {
                        log.error(fileOriginalName + " Clear Back X Days Part2", e);
                    }
                }
            } catch (e) {
                log.error("Field id: " + fileId + " Part1: Exception on save process status record", e);
            } finally {
                if (fileId) {
                    // Set status back to Rejected.
                    if (processStatusRecInfo && processStatusRecInfo.poId && processStatusRecInfo.poStatus == "3" && processStatusRecInfo.poStatusChanged) {
                        const values = {
                            approvalstatus: "3"
                        };
                        values[DD_CONSTANT.CUSTOM_FIELD_ID.AUTO_APPROVAL_DASH_MART] = true;
                        record.submitFields({
                            type: record.Type.PURCHASE_ORDER,
                            id: processStatusRecInfo.poId,
                            values: values
                        });
                    }

                    ddt.logForProcessHistory(processStatusRecInfo, startTime);
                    mapContext.write({
                        key: fileId,
                        value: {
                            processStatusRecInfo: processStatusRecInfo,
                            processedHistoryRecIdId: processedHistoryRecId
                        }
                    });
                }
            }
        }

        /**
         * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
         * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
         * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
         *     provided automatically based on the results of the map stage.
         * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
         *     reduce function on the current group
         * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
         * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} reduceContext.key - Key to be processed during the reduce stage
         * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
         *     for processing
         * @since 2015.2
         */
        const reduce = (reduceContext) => {
            // Usage limit 5000
            const fileId = reduceContext.key;
            try {
                const processedRecArr = reduceContext.values;
                let generatedRecordIdArr = [];
                let generatedRecordInfo = {};
                let generatedRecordInvoiceInfoArr = [];
                let originalFileName = "";
                let allInvoiceSuccess = true;
                let failCount = 0;
                let successCount = 0;
                let totalCounts = 0;
                let matchedFileInfo = "";
                let failedInvoiceInfo = {};
                let successInvoiceInfo = {};
                let processedTime = 0;
                let matchedFileRecId = "";
                let successCabinetId = "";
                let searchMatchedFileInfo = true;
                let hasSuccessProcessedRec = false;
                let processedHistoryRecIdIdArr = [];
                let doordashConfigureInfo = "";
                // image that one file with one vendor
                let vendorId = "";
                let vendorName = "";
                let allSkipped = true;
                let poNum = "";
                let poId = "";
                let duplicateInvoiceProcessHisArr = [];
                let errorCodeInfo = {};
                let duplicateNewFile = false;
                const typeCodeArr = [];
                const invoiceNumArr = [];
                const tdsTotalArr = [];
                let successProcessedRec = "";
              let invDate = "";
              let invoiceDt="";
                try {
                    processedRecArr && processedRecArr.length > 0 && processedRecArr.forEach(processInfoString => {
                        const processedInfo = JSON.parse(processInfoString);
                        const processStatusInfo = processedInfo.processStatusRecInfo;
                        const processedHistoryRecIdId = processedInfo.processedHistoryRecIdId;
                        processedHistoryRecIdId && processedHistoryRecIdIdArr.push(processedHistoryRecIdId);
                        originalFileName = processStatusInfo.fileName;
                        successCabinetId = processStatusInfo.successCabinetId;
                        vendorId = processStatusInfo.vendorId;
                        vendorName = processStatusInfo.vendorName;
                        poNum = processStatusInfo.poNumber;
                        poId = processStatusInfo.poId;
                        invDate = processStatusInfo.originalInvoiceDate
                      if(invDate)
                      {
                        invoiceDt = convertToDateObject(invDate);
                      }
                        processStatusInfo.typeCode && typeCodeArr.push(processStatusInfo.typeCode);
                        processStatusInfo.invoiceNumber && invoiceNumArr.push(processStatusInfo.invoiceNumber);
                        processStatusInfo.invoiceTotal && tdsTotalArr.push(processStatusInfo.invoiceTotal);
                        const errorCode = processStatusInfo.errorCode;
                        if (!errorCodeInfo[errorCode]) {
                            errorCodeInfo[errorCode] = 1;
                        } else {
                            errorCodeInfo[errorCode] += 1;
                        }
                        // duplicate invoice information
                        processStatusInfo.duplicateInvoice && duplicateInvoiceProcessHisArr.push({
                            processHistoryInfo: processStatusInfo
                        });
                        doordashConfigureInfo = processStatusInfo.doordashConfigureInfo;
                        // load matched file info
                        if (searchMatchedFileInfo){
                            searchMatchedFileInfo = false;
                            matchedFileInfo = ddt.getMatchedFileRecordInfo(originalFileName, processStatusInfo.comeFromFinTech ? processStatusInfo.invoiceNumber : "", processStatusInfo.senderId);
                            if (matchedFileInfo){
                                matchedFileRecId = matchedFileInfo.id;
                                successCount = Number(matchedFileInfo.successCount);
                                failCount = Number(matchedFileInfo.failureCount);
                                totalCounts = Number(matchedFileInfo.totalCount);
                                processedTime = Number(matchedFileInfo.processedTime);
                                failedInvoiceInfo = matchedFileInfo.failureInvoiceInfo && JSON.parse(matchedFileInfo.failureInvoiceInfo);
                                successInvoiceInfo = matchedFileInfo.successInvoiceInfo && JSON.parse(matchedFileInfo.successInvoiceInfo);
                            }
                        }
                        if (processStatusInfo.processStatus != DD_CONSTANT.HISTORY_PROCESS_STATUS.SKIPPED) {
                            allSkipped = false;
                        }
                        if (processStatusInfo.duplicateInvoice && processStatusInfo.newFile && !processStatusInfo.duplicateHandled) {
                            duplicateNewFile = true;
                        }
                        if (processStatusInfo.processStatus != DD_CONSTANT.HISTORY_PROCESS_STATUS.SKIPPED && (!processStatusInfo.duplicateInvoice
                            || processStatusInfo.duplicateHandled)){
                            // Append current processed info for invoice file
                            if (processStatusInfo.processStatus == DD_CONSTANT.HISTORY_PROCESS_STATUS.SUCCESS){
                                hasSuccessProcessedRec = true;
                                successProcessedRec = processStatusInfo;
                                generatedRecordIdArr = generatedRecordIdArr.concat(processStatusInfo.vendorBillIdArr);
                                if (processStatusInfo.vendorBillIdInfo){
                                    for (const id in processStatusInfo.vendorBillIdInfo) {
                                        generatedRecordInfo[id] = processStatusInfo.vendorBillIdInfo[id];
                                    }
                                }
                                generatedRecordInvoiceInfoArr.push(processStatusInfo);
                                if (!successInvoiceInfo[processStatusInfo.invoiceNumber]) {
                                    successInvoiceInfo[processStatusInfo.invoiceNumber] = true;
                                    successCount++;
                                }
                                if (matchedFileInfo){
                                    // remove success processed invoice from failure list
                                    if (failedInvoiceInfo[processStatusInfo.invoiceNumber]){
                                        delete failedInvoiceInfo[processStatusInfo.invoiceNumber];
                                        failCount > 0 && failCount--;
                                    }
                                }
                            } else {
                                // Only store new failed invoice
                                if (!failedInvoiceInfo[processStatusInfo.invoiceNumber]){
                                    failedInvoiceInfo[processStatusInfo.invoiceNumber] = true;
                                    failCount++;
                                }
                                allInvoiceSuccess = false;
                            }
                        }
                    });
                } catch (e) {
                    log.error("fileId:" + fileId + " Reduce Exception Part1", e);
                }

                // Calculate total count for file
                totalCounts = successCount + failCount;
                let fileStatus = "";
                // Move file process
                const invoiceEdiFile = fileId && file.load({id: fileId});
                if (errorCodeInfo[DD_CONSTANT.ERROR_CODE.PERMANENT_FAIL] && Object.keys(errorCodeInfo).length == 1) {
                    // Move file to Permanent Folder
                    ddt.saveToCabinet(invoiceEdiFile, doordashConfigureInfo.permanentCabinetId, vendorName, originalFileName);
                    fileStatus = DD_CONSTANT.FILE_PROCESS_STATUS.PERMANENT_FAIL;
                } else {
                    if (allSkipped) {
                        const skippedCabinetId = doordashConfigureInfo.skippedCabinetId;
                        // Move to skipped folder
                        ddt.saveToCabinet(invoiceEdiFile, skippedCabinetId, vendorName, originalFileName);
                        fileStatus = DD_CONSTANT.FILE_PROCESS_STATUS.SKIPPED;
                    } else {
                        if ((successCount == totalCounts && totalCounts > 0) || duplicateNewFile){
                            // Move to success folder
                            ddt.saveToCabinet(invoiceEdiFile, successCabinetId, vendorName, originalFileName);
                        }
                        fileStatus = successCount == totalCounts ? DD_CONSTANT.FILE_PROCESS_STATUS.SUCCESS : DD_CONSTANT.FILE_PROCESS_STATUS.FAILURE;
                    }
                }

                // Create File Info Record
                const fileInfoRecType = DD_CONSTANT.CUSTOM_RECORD_TYPE_ID.EDI_FILE_INFO_RECORD_TYPE_ID;
                let fileInfoRecord = "";
                if (matchedFileRecId){
                    fileInfoRecord = record.load({type: fileInfoRecType, id: matchedFileRecId});
                    fileInfoRecord.setValue({fieldId: "name", value: originalFileName});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_file_process_status", value: fileStatus});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_file", value: fileId});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_file_vendor", value: vendorId});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_processed_time", value: ++processedTime});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_invoice_count", value: totalCounts});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_success_count", value: successCount});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_failure_count", value: failCount});
                    fileInfoRecord.setValue({fieldId: "custrecord_success_invoice_info", value: successInvoiceInfo ? JSON.stringify(successInvoiceInfo) : ""});
                    fileInfoRecord.setValue({fieldId: "custrecord_failure_invoice_info", value: failedInvoiceInfo ? JSON.stringify(failedInvoiceInfo) : ""});
                } else {
                    fileInfoRecord = record.create({type: fileInfoRecType});
                    fileInfoRecord.setValue({fieldId: "name", value: originalFileName});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_file", value: fileId});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_file_vendor", value: vendorId});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_file_process_status", value: fileStatus});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_processed_time", value: ++processedTime});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_invoice_count", value: totalCounts});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_success_count", value: successCount});
                    fileInfoRecord.setValue({fieldId: "custrecord_dd_failure_count", value: failCount});
                    fileInfoRecord.setValue({fieldId: "custrecord_success_invoice_info", value: successInvoiceInfo ? JSON.stringify(successInvoiceInfo) : ""});
                    fileInfoRecord.setValue({fieldId: "custrecord_failure_invoice_info", value: failedInvoiceInfo ? JSON.stringify(failedInvoiceInfo) : ""});
                }
                const fileInfoRecId = fileInfoRecord.save({ignoreMandatoryFields: true});

                // Create Processed History Record for success    one success record for each executing (Contains multiple successful invoices, generated vendor bill fields is multiple select)
                if (hasSuccessProcessedRec && (!matchedFileRecId || generatedRecordIdArr.length > 0)){
                    const processStatusRec = record.create({type: DD_CONSTANT.CUSTOM_RECORD_TYPE_ID.EDI_HISTORY_RECORD_TYPE_ID, isDynamic: true});
                    processStatusRec.setValue({fieldId: "custrecord_dd_source_file", value: fileId});
                  
                    processStatusRec.setValue({fieldId: "custrecord_dd_his_edi_type", value: DD_CONSTANT.EDI_TYPE["810"]});
                    const successStatus = successProcessedRec.successCode == DD_CONSTANT.SUCCESS_CODE.WARNING ? DD_CONSTANT.HISTORY_PROCESS_STATUS.SUCCESS_WITH_WARNING : DD_CONSTANT.HISTORY_PROCESS_STATUS.SUCCESS;
                    processStatusRec.setValue({fieldId: "custrecord_dd_process_status", value: successStatus});
                    processStatusRec.setValue({fieldId: "custrecord_dd_generated_vendor_bills", value: generatedRecordIdArr});
                    processStatusRec.setValue({fieldId: "custrecord_dd_txn_internal_id", value: generatedRecordIdArr.length==1?generatedRecordIdArr.toString():""});
                    processStatusRec.setValue({fieldId: "custrecord_dd_source_file_name", value: originalFileName});
                    processStatusRec.setValue({fieldId: "custrecord_dd_record_counts", value: generatedRecordIdArr.length});
                    processStatusRec.setValue({fieldId: "custrecord_dd_edi_file_info_parent", value: fileInfoRecId});
                    processStatusRec.setValue({fieldId: "custrecord_dd_purchase_order_number", value: poNum});
                    processStatusRec.setValue({fieldId: "custrecord_dd_matched_netsuite_po", value: poId || ""});
                    processStatusRec.setValue({fieldId: "custrecord_dd_invoice_date", value: invoiceDt || ""});
                    processStatusRec.setValue({fieldId: "custrecord_dd_invoice_number", value: invoiceNumArr.length == 1 ? invoiceNumArr[0] : ""});
                    processStatusRec.setValue({fieldId: "custrecord_dd_vendor", value: vendorId});
                    processStatusRec.setValue({fieldId: "custrecord_dd_transaction_type_code", value: typeCodeArr.length == 1 ? typeCodeArr[0] : ""});
                    processStatusRec.setValue({fieldId: "custrecord_dd_tds_total", value: tdsTotalArr.length == 1 ? tdsTotalArr[0] : ""});
                    processStatusRec.setValue({fieldId: "custrecord_dd_pro_his_location", value: successProcessedRec.locationInfo ? successProcessedRec.locationInfo.id : ""});
                    successProcessedRec.successCode == DD_CONSTANT.SUCCESS_CODE.WARNING && processStatusRec.setValue({fieldId: "custrecord_dd_reason_for_failure", value: successProcessedRec.warning});
                    processStatusRec.save({ignoreMandatoryFields: true});
                    try {
                        // Attach file to corresponding bill/credit record
                        if (generatedRecordIdArr && generatedRecordIdArr.length > 0) {
                            generatedRecordIdArr.forEach(recordId => {
                                const recordInfo = generatedRecordInfo[recordId];
                                recordId && recordInfo && record.attach({
                                    record: {
                                        type: "file",
                                        id: fileId
                                    },
                                    to: {
                                        type: recordInfo.type,
                                        id: recordId
                                    },
                                    attributes: {
                                        role: 3
                                    }
                                });
                            })
                        }

                        // Correcting exist historic processing record
                        ddt.correctExistHistoricalRecord(matchedFileRecId, fileInfoRecId, processedTime
                            , generatedRecordInvoiceInfoArr, doordashConfigureInfo);
                    } catch (e) {
                        log.error("fileId:" + fileId + "Reduce generatedRecordInvoiceInfoArr ", generatedRecordInvoiceInfoArr);
                        log.error("fileId:" + fileId + "Reduce ddt.correctExistHistoricalRecor Exception", e);
                    }
                }

                // Linked to FILE INFO RECORD FOR FAILURE HISTORY RECORD
                reduceContext.write({key: fileInfoRecId, value: processedHistoryRecIdIdArr});
            } catch (e) {
                log.error("fileId:" + fileId + "Reduce Exception", e);
            }
        }


        /**
         * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
         * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
         * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
         * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
         *     script
         * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
         * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
         * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
         * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
         *     script
         * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
         * @param {Object} summaryContext.inputSummary - Statistics about the input stage
         * @param {Object} summaryContext.mapSummary - Statistics about the map stage
         * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
         * @since 2015.2
         */
        const summarize = (summaryContext) => {
            // Usage limit 10000
            // Linked to corresponding FILE INFO RECORD for new failure history record
            summaryContext.output.iterator().each(function (fileInfoRecId, processedHistoryRecIdArrStr){
                const processedHistoryRecIdArr = JSON.parse(processedHistoryRecIdArrStr);
                // Set edi file info for failure history record
                if (processedHistoryRecIdArr && processedHistoryRecIdArr.length > 0){
                    processedHistoryRecIdArr.forEach(recId => {
                        // usage 2
                        record.submitFields({type: DD_CONSTANT.CUSTOM_RECORD_TYPE_ID.EDI_HISTORY_RECORD_TYPE_ID, id: recId, values: {
                                custrecord_dd_edi_file_info_parent: fileInfoRecId
                            }});
                    })
                }
                return true;
            });
        }
        const convertToDateObject = (ccYyMmDdFormatDateString) => {
            if (!ccYyMmDdFormatDateString || ccYyMmDdFormatDateString.length != 8){
                return "";
            }

            const dateComponentArr = ccYyMmDdFormatDateString.match(/(\d{4})(\d{2})(\d{2})/);
            return new Date(dateComponentArr[1], dateComponentArr[2] - 1, dateComponentArr[3]);
        }

        return {
            getInputData,
            map,
            reduce,
            summarize
        }

    });