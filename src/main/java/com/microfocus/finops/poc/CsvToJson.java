package com.microfocus.finops.poc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CsvToJson {

    Logger logger = LoggerFactory.getLogger(CsvToJson.class);

    private static int NUM_OF_RECORDS_IN_PAYLOAD = 200;

    private OdlConfiguration configuration;

    public static void main(String[] args) throws Exception {
        CsvToJson csvToJson = new CsvToJson();
        csvToJson.start();
    }

    public CsvToJson() {
        try {
            configuration = new OdlConfiguration();
        } catch (IOException e) {
            logger.error("Error", e);
        }
    }

    private void start() throws IOException {
        Instant start = Instant.now();
        csvToJsonConvertion2();
        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        logger.info("Time taken: " + timeElapsed.toMillis() + " milliseconds");
        logger.info("Time taken: " + timeElapsed.getSeconds() + " seconds");
    }


    public void csvToJsonConvertion2() throws IOException {
        File awsCsvDir = new File("aws_csv");
        File odlPayloadsDir = new File("aws_json");

        if (!awsCsvDir.exists()) {
            awsCsvDir.mkdirs();
            logger.error("There are no source csv file to convert");
            System.exit(0);
        }

        if (!odlPayloadsDir.exists() || !odlPayloadsDir.isDirectory()) {
            logger.info("The target directory of aws json is not present. Creating aws_json directory");
            odlPayloadsDir.mkdirs();
        }

        if (odlPayloadsDir.exists() && odlPayloadsDir.isDirectory() && odlPayloadsDir.list() != null) {
            logger.info("Cleaning old aws json payload files.");
            FileUtils.cleanDirectory(odlPayloadsDir);
        }

        NUM_OF_RECORDS_IN_PAYLOAD = configuration.getMessagesPerBatch();
        File[] allCsvFiles = awsCsvDir.listFiles();
        for (File csvFile : allCsvFiles) {
            if (csvFile.getName().endsWith(".csv")) {
                split(csvFile, NUM_OF_RECORDS_IN_PAYLOAD);
            }
        }
    }

    public void split(File file, long nol) throws IOException {

        try {
            if (!file.exists() || !file.canRead() || !file.isFile()) {
                logger.error("Error processing the input file :" + file);
                return;
            }

            Instant start = Instant.now();
            // Reading file and getting no. of files to be generated

            //File file = new File(inputfile);
            int count = Util.lineCount(file);
            ;

            logger.info("Lines in the file " + file.getName() + " : " + count + "(including header)");

            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            logger.info("Time taken to count lines: " + timeElapsed.toMillis() + " milliseconds");
            logger.info("Time taken to count lines: " + timeElapsed.getSeconds() + " seconds");

            int linesCovered = 0;
            FileInputStream inputfstream = new FileInputStream(file);
            DataInputStream in = new DataInputStream(inputfstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            int fileNumberCount = 0;
            String header = "identity/LineItemId,identity/TimeInterval,bill/InvoiceId,bill/InvoicingEntity,bill/BillingEntity,bill/BillType,bill/PayerAccountId,bill/BillingPeriodStartDate,bill/BillingPeriodEndDate,lineItem/UsageAccountId,lineItem/LineItemType,lineItem/UsageStartDate,lineItem/UsageEndDate,lineItem/ProductCode,lineItem/UsageType,lineItem/Operation,lineItem/AvailabilityZone,lineItem/ResourceId,lineItem/UsageAmount,lineItem/NormalizationFactor,lineItem/NormalizedUsageAmount,lineItem/CurrencyCode,lineItem/UnblendedRate,lineItem/UnblendedCost,lineItem/BlendedRate,lineItem/BlendedCost,lineItem/LineItemDescription,lineItem/TaxType,lineItem/LegalEntity,product/ProductName,product/PurchaseOption,product/accessType,product/alarmType,product/apiType,product/attachmentType,product/availability,product/availabilityZone,product/awsresource,product/backupEvaluations,product/backupservice,product/baseProductReferenceCode,product/brokerEngine,product/bundle,product/bundleDescription,product/bundleGroup,product/cacheEngine,product/cacheMemorySizeGb,product/cacheType,product/capacity,product/capacitystatus,product/category,product/ciType,product/classicnetworkingsupport,product/clientLocation,product/clockSpeed,product/component,product/computeFamily,product/computeType,product/connectionType,product/contentType,product/countsAgainstQuota,product/cputype,product/currentGeneration,product/data,product/dataTransfer,product/dataTransferQuota,product/dataType,product/databaseEdition,product/databaseEngine,product/datatransferout,product/dedicatedEbsThroughput,product/deploymentOption,product/describes,product/description,product/deviceOs,product/directConnectLocation,product/directorySize,product/directoryType,product/directoryTypeDescription,product/dominantnondominant,product/durability,product/ecu,product/edition,product/endpoint,product/endpointType,product/engine,product/engineCode,product/enhancedInfrastructureMetrics,product/enhancedNetworkingSupport,product/enhancedNetworkingSupported,product/entityType,product/equivalentondemandsku,product/eventType,product/executionMode,product/externalInstanceType,product/feeCode,product/feeDescription,product/fileSystemType,product/findingGroup,product/findingSource,product/findingStorage,product/freeOverage,product/freeQueryTypes,product/freeTier,product/freeTrial,product/freeUsageIncluded,product/fromLocation,product/fromLocationType,product/fromRegionCode,product/fromcountry,product/gets,product/gpu,product/gpuMemory,product/granularity,product/group,product/groupDescription,product/highAvailability,product/inputMode,product/insightstype,product/instance,product/instanceCapacity2xlarge,product/instanceCapacity4xlarge,product/instanceCapacityLarge,product/instanceCapacityMedium,product/instanceCapacityMetal,product/instanceCapacityXlarge,product/instanceFamily,product/instanceFunction,product/instanceName,product/instanceType,product/instanceTypeFamily,product/instances,product/instancesku,product/intelAvx2Available,product/intelAvxAvailable,product/intelTurboAvailable,product/io,product/ioRequestType,product/license,product/licenseModel,product/location,product/locationType,product/logsDestination,product/machineLearningProcess,product/mailboxStorage,product/marketoption,product/maxIopsBurstPerformance,product/maxIopsvolume,product/maxThroughputvolume,product/maxVolumeSize,product/maximumExtendedStorage,product/maximumStorageVolume,product/memory,product/memoryGib,product/memorytype,product/messageDeliveryFrequency,product/messageDeliveryOrder,product/meterMode,product/meteringType,product/minVolumeSize,product/minimumStorageVolume,product/networkPerformance,product/normalizationSizeFactor,product/numbertype,product/operatingSystem,product/operation,product/opsItems,product/origin,product/osLicenseModel,product/outputMode,product/overageType,product/parameterType,product/physicalCores,product/physicalCpu,product/physicalGpu,product/physicalProcessor,product/platoclassificationtype,product/platodataanalyzedtype,product/platofeaturetype,product/platoinstancename,product/platoinstancetype,product/platopagedatatype,product/platopricingtype,product/platoprotectionpolicytype,product/platoprotocoltype,product/platostoragename,product/platostoragetype,product/platotrafficdirection,product/platotransfertype,product/platousagetype,product/platovolumetype,product/portSpeed,product/preInstalledSw,product/pricingUnit,product/primaryplaceofuse,product/processorArchitecture,product/processorFeatures,product/productFamily,product/productSchemaDescription,product/productType,product/provisioned,product/purchaseterm,product/queueType,product/recipient,product/region,product/regionCode,product/replicationType,product/requestDescription,product/requestType,product/resourceAssessment,product/resourceEndpoint,product/resourcePriceGroup,product/resourceType,product/rootvolume,product/routingTarget,product/routingType,product/runningMode,product/scanType,product/servicecode,product/servicename,product/size,product/sku,product/snapshotarchivefeetype,product/softwareIncluded,product/softwareType,product/standardGroup,product/standardStorage,product/standardStorageRetentionIncluded,product/steps,product/storage,product/storageClass,product/storageFamily,product/storageMedia,product/storageType,product/subcategory,product/subscriptionType,product/tenancy,product/throughput,product/throughputCapacity,product/throughputClass,product/tickettype,product/tier,product/tiertype,product/toLocation,product/toLocationType,product/toRegionCode,product/trafficDirection,product/transactionType,product/transferType,product/type,product/updates,product/usageFamily,product/usageTier,product/usageVolume,product/usagetype,product/uservolume,product/vcpu,product/version,product/videoMemoryGib,product/volumeApiName,product/volumeType,product/vpcnetworkingsupport,product/withActiveUsers,pricing/LeaseContractLength,pricing/OfferingClass,pricing/PurchaseOption,pricing/RateCode,pricing/RateId,pricing/currency,pricing/publicOnDemandCost,pricing/publicOnDemandRate,pricing/term,pricing/unit,reservation/AmortizedUpfrontCostForUsage,reservation/AmortizedUpfrontFeeForBillingPeriod,reservation/EffectiveCost,reservation/EndTime,reservation/ModificationStatus,reservation/NormalizedUnitsPerReservation,reservation/NumberOfReservations,reservation/RecurringFeeForUsage,reservation/ReservationARN,reservation/StartTime,reservation/SubscriptionId,reservation/TotalReservedNormalizedUnits,reservation/TotalReservedUnits,reservation/UnitsPerReservation,reservation/UnusedAmortizedUpfrontFeeForBillingPeriod,reservation/UnusedNormalizedUnitQuantity,reservation/UnusedQuantity,reservation/UnusedRecurringFee,reservation/UpfrontValue,savingsPlan/TotalCommitmentToDate,savingsPlan/SavingsPlanARN,savingsPlan/SavingsPlanRate,savingsPlan/UsedCommitment,savingsPlan/SavingsPlanEffectiveCost,savingsPlan/AmortizedUpfrontCommitmentForBillingPeriod,savingsPlan/RecurringCommitmentForBillingPeriod,savingsPlan/StartTime,savingsPlan/EndTime,savingsPlan/InstanceTypeFamily,savingsPlan/OfferingType,savingsPlan/PaymentOption,savingsPlan/PurchaseTerm,savingsPlan/Region,costCategory/LandingZone,costCategory/Transformation Cost Category";
            while (linesCovered < count) {
                int count1 = 0;
                StringBuilder csvContentBuilder = new StringBuilder();
                csvContentBuilder.append(header);

                while (count1 < nol && linesCovered < count) {
                    count1++;
                    String str = br.readLine();
                    linesCovered++;
                    if (!str.contains("identity/LineItemId")) {
                        csvContentBuilder.append("\n").append(str);
                    }
                }

                String jsonFileName = file.getName();
                jsonFileName = jsonFileName.substring(0, jsonFileName.lastIndexOf("."));
                jsonFileName = jsonFileName + "-" + byPaddingZeros(fileNumberCount, 4) + ".json";

                csvToJsonConversionStr(csvContentBuilder.toString(), "aws_json/" + jsonFileName);
                fileNumberCount++;
            }
            in.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    public void csvToJsonConvertion(String csvFileName, String jsonFileName) throws IOException {
        File input = new File(csvFileName);
        File output = new File(jsonFileName);

        List<Map<String, String>> data = readObjectsFromCsv(input);
        writeAsJson(data, output);
    }

    public List<Map<String, String>> readObjectsFromCsv(File file) throws IOException {
        CsvSchema bootstrap = CsvSchema.emptySchema().withHeader();
        CsvMapper csvMapper = new CsvMapper();
        try (MappingIterator<Map<String, String>> mappingIterator = csvMapper.readerFor(Map.class).with(bootstrap).readValues(file)) {
            return mappingIterator.readAll();
        }
    }

    public void writeAsJson(List<Map<String, String>> data, File file) throws IOException {
        List<Map<?, ?>> newData = new ArrayList<>();
        for (Map<String, String> map : data) {
            CollectionUtils.filter(map.values(), new Predicate<String>() {
                @Override
                public boolean evaluate(String s) {
                    return (s != null && !s.trim().isEmpty());
                }
            });

            newData.add(map);
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.writeValue(file, data);
    }

    public void csvToJsonConversionStr(String str, String jsonFileName) throws IOException {
        File output = new File(jsonFileName);
        logger.info("Writing json to file : " + output.getName());
        List<Map<String, String>> data = readObjectsFromStr(str);
        writeAsJson(data, output);
    }

    public static List<Map<String, String>> readObjectsFromStr(String str) throws IOException {
        CsvSchema bootstrap = CsvSchema.emptySchema().withHeader();
        CsvMapper csvMapper = new CsvMapper();
        csvMapper.enable(CsvParser.Feature.IGNORE_TRAILING_UNMAPPABLE);
        try (MappingIterator<Map<String, String>> mappingIterator = csvMapper.readerFor(Map.class).with(bootstrap).readValues(str)) {
            return mappingIterator.readAll();
        }
    }

    public static String byPaddingZeros(int value, int paddingLength) {
        return String.format("%0" + paddingLength + "d", value);
    }
}