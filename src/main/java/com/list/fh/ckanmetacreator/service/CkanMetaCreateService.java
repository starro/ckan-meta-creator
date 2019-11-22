package com.list.fh.ckanmetacreator.service;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;

@Slf4j
@Service
public class CkanMetaCreateService {
    public static final Logger logger = LoggerFactory.getLogger(CkanMetaCreateService.class);

    public void createCkanMeta(String source, String target) throws Exception {

        File csvFile = new File(source);

        String file_path = target;
        int lastIndex = file_path.lastIndexOf(File.separator);
        String fileName = file_path.substring(lastIndex + 1);
        String filePath = file_path.substring(0, lastIndex + 1);

        logger.info("file path: " + filePath);
        logger.info("file name: " + fileName);

        File targetPath = new File(filePath);
        if (!targetPath.exists()) {
            logger.info("Create file path");
            targetPath.mkdirs();
        }

        CsvSchema csvSchema = CsvSchema.builder().setUseHeader(true).build();
        CsvMapper csvMapper = new CsvMapper();

        // Read data from CSV file
        List<Object> readAll = csvMapper.readerFor(Map.class).with(csvSchema).readValues(csvFile).readAll();


        MappingIterator<Map<String, String>> ckanMetaData = csvMapper.readerFor(Map.class)
                .with(csvSchema)
                .readValues(csvFile);

        logger.info("All List: " + readAll.toString());

        ObjectMapper mapper = new ObjectMapper();

        Map<String, Object> primeMap = new LinkedHashMap<String, Object>();
        primeMap.putAll(ckanMetaData.next());

        //키워드는 쉼표로 분리한다.
        String keywords = primeMap.get("DATASET_keyword").toString();
        String[] keywordStrings = keywords.split(",");

        //CKAN 메타 keyword(tag)에 포함될 디캣 속성들을 정의한다.
        List<LinkedHashMap<String, String>> taglist = new ArrayList<>();

        for (String keyword : keywordStrings) {
            LinkedHashMap<String, String> keyMap = new LinkedHashMap<String, String>();

            keyMap.put("name", keyword.trim());
            taglist.add(keyMap);
        }

        primeMap.put("tags", taglist);

        //CKAN 메타 extras에 포함될 디캣 속성들을 정의한다.
        List<LinkedHashMap<String, String>> extraValueList = new ArrayList<LinkedHashMap<String, String>>();

        //conformsTo 속성들
        LinkedHashMap<String, String> conformsToKeyAndValueMap = new LinkedHashMap<String, String>();
        conformsToKeyAndValueMap.put("key", "conformsTo");
        conformsToKeyAndValueMap.put("value", primeMap.get("DATASET_conformsTo").toString());
        extraValueList.add(conformsToKeyAndValueMap);

        //contactName(author) 속성들
        LinkedHashMap<String, String> contactNameKeyAndValueMap = new LinkedHashMap<String, String>();
        contactNameKeyAndValueMap.put("key", "contactName");
        contactNameKeyAndValueMap.put("value", primeMap.get("CKAN_author").toString());
        extraValueList.add(contactNameKeyAndValueMap);

        //contactPoint 속성들
        LinkedHashMap<String, String> contactPointKeyAndValueMap = new LinkedHashMap<String, String>();
        contactPointKeyAndValueMap.put("key", "contactPoint");
        contactPointKeyAndValueMap.put("value", primeMap.get("DATASET_contactPoint").toString());
        extraValueList.add(contactPointKeyAndValueMap);

        //creator 속성들
        LinkedHashMap<String, String> creatorKeyAndValueMap = new LinkedHashMap<String, String>();
        creatorKeyAndValueMap.put("key", "creator");
        creatorKeyAndValueMap.put("value", primeMap.get("DATASET_creator").toString());
        extraValueList.add(creatorKeyAndValueMap);

        //publisher 속성들
        LinkedHashMap<String, String> publisherKeyAndValueMap = new LinkedHashMap<String, String>();
        publisherKeyAndValueMap.put("key", "publisher");
        publisherKeyAndValueMap.put("value", primeMap.get("DATASET_publisher").toString());
        extraValueList.add(publisherKeyAndValueMap);

        //relation 속성들
        LinkedHashMap<String, String> relationKeyAndValueMap = new LinkedHashMap<String, String>();
        relationKeyAndValueMap.put("key", "relation");
        relationKeyAndValueMap.put("value", primeMap.get("DATASET_relation").toString());
        extraValueList.add(relationKeyAndValueMap);

        //type 속성들
        LinkedHashMap<String, String> typeKeyAndValueMap = new LinkedHashMap<String, String>();
        typeKeyAndValueMap.put("key", "dcat_type");
        typeKeyAndValueMap.put("value", primeMap.get("DATASET_type").toString());
        extraValueList.add(typeKeyAndValueMap);

        //distribution 속성들
        LinkedHashMap<String, String> distributionKeyAndValueMap = new LinkedHashMap<String, String>();
        distributionKeyAndValueMap.put("key", "distribution");
        distributionKeyAndValueMap.put("value", primeMap.get("DATASET_distribution").toString());
        extraValueList.add(distributionKeyAndValueMap);

        //language 속성들
        LinkedHashMap<String, String> languageKeyAndValueMap = new LinkedHashMap<String, String>();
        languageKeyAndValueMap.put("key", "language");
        languageKeyAndValueMap.put("value", primeMap.get("DATASET_language").toString());
        extraValueList.add(languageKeyAndValueMap);

        //spatial 속성들
        LinkedHashMap<String, String> spatialKeyAndValueMap = new LinkedHashMap<String, String>();
        spatialKeyAndValueMap.put("key", "spatial");
        spatialKeyAndValueMap.put("value", primeMap.get("DATASET_spatial").toString());
        extraValueList.add(spatialKeyAndValueMap);

        //distribution_title 속성들
        LinkedHashMap<String, String> distribution_titleKeyAndValueMap = new LinkedHashMap<String, String>();
        distribution_titleKeyAndValueMap.put("key", "distribution_title");
        distribution_titleKeyAndValueMap.put("value", primeMap.get("Distribution_title").toString());
        extraValueList.add(distribution_titleKeyAndValueMap);

        //distribution_description 속성들
        LinkedHashMap<String, String> distribution_descriptionKeyAndValueMap = new LinkedHashMap<String, String>();
        distribution_descriptionKeyAndValueMap.put("key", "distribution_description");
        distribution_descriptionKeyAndValueMap.put("value", primeMap.get("Distribution_description").toString());
        extraValueList.add(distribution_descriptionKeyAndValueMap);

        //distribution_issued 속성들
        LinkedHashMap<String, String> distribution_issuedKeyAndValueMap = new LinkedHashMap<String, String>();
        distribution_issuedKeyAndValueMap.put("key", "distribution_issued");
        distribution_issuedKeyAndValueMap.put("value", primeMap.get("Distribution_issued").toString());
        extraValueList.add(distribution_issuedKeyAndValueMap);

        //distribution_license 속성들
        LinkedHashMap<String, String> distribution_licenseKeyAndValueMap = new LinkedHashMap<String, String>();
        distribution_licenseKeyAndValueMap.put("key", "distribution_license");
        distribution_licenseKeyAndValueMap.put("value", primeMap.get("Distribution_license").toString());
        extraValueList.add(distribution_licenseKeyAndValueMap);

        //distribution_accessURL 속성들
        LinkedHashMap<String, String> distribution_accessURLKeyAndValueMap = new LinkedHashMap<String, String>();
        distribution_accessURLKeyAndValueMap.put("key", "distribution_accessURL");
        distribution_accessURLKeyAndValueMap.put("value", primeMap.get("Distribution_accessURL").toString());
        extraValueList.add(distribution_accessURLKeyAndValueMap);

        //distribution_accessService 속성들
        LinkedHashMap<String, String> distribution_accessServiceKeyAndValueMap = new LinkedHashMap<String, String>();
        distribution_accessServiceKeyAndValueMap.put("key", "distribution_accessService");
        distribution_accessServiceKeyAndValueMap.put("value", primeMap.get("Distribution_accessService").toString());
        extraValueList.add(distribution_accessServiceKeyAndValueMap);

        //distribution_downloadURL 속성들
        LinkedHashMap<String, String> distribution_downloadURLKeyAndValueMap = new LinkedHashMap<String, String>();
        distribution_downloadURLKeyAndValueMap.put("key", "distribution_downloadURL");
        distribution_downloadURLKeyAndValueMap.put("value", primeMap.get("Distribution_downloadURL").toString());
        extraValueList.add(distribution_downloadURLKeyAndValueMap);

        //distribution_spatialResolutionlnMeters 속성들
        LinkedHashMap<String, String> distribution_spatialResolutionlnMetersKeyAndValueMap = new LinkedHashMap<String, String>();
        distribution_spatialResolutionlnMetersKeyAndValueMap.put("key", "distribution_spatialResolutionlnMeters");
        distribution_spatialResolutionlnMetersKeyAndValueMap.put("value", primeMap.get("Distribution_spatialResolutionlnMeters").toString());
        extraValueList.add(distribution_spatialResolutionlnMetersKeyAndValueMap);

        //distribution_format 속성들
        LinkedHashMap<String, String> distribution_formatKeyAndValueMap = new LinkedHashMap<String, String>();
        distribution_formatKeyAndValueMap.put("key", "distribution_format");
        distribution_formatKeyAndValueMap.put("value", primeMap.get("Distribution_format").toString());
        extraValueList.add(distribution_formatKeyAndValueMap);

        //distribution_compressFormat 속성들
        LinkedHashMap<String, String> distribution_compressFormatKeyAndValueMap = new LinkedHashMap<String, String>();
        distribution_compressFormatKeyAndValueMap.put("key", "distribution_compressFormat");
        distribution_compressFormatKeyAndValueMap.put("value", primeMap.get("Distribution_compressFormat").toString());
        extraValueList.add(distribution_compressFormatKeyAndValueMap);

        //dataservice_endpointURL 속성들
        LinkedHashMap<String, String> dataservice_endpointURLKeyAndValueMap = new LinkedHashMap<String, String>();
        dataservice_endpointURLKeyAndValueMap.put("key", "dataservice_endpointURL");
        dataservice_endpointURLKeyAndValueMap.put("value", primeMap.get("DataService_endpointURL").toString());
        extraValueList.add(dataservice_endpointURLKeyAndValueMap);

        //dataService_endpointDescripton 속성들
        LinkedHashMap<String, String> dataService_endpointDescriptonKeyAndValueMap = new LinkedHashMap<String, String>();
        dataService_endpointDescriptonKeyAndValueMap.put("key", "dataService_endpointDescripton");
        dataService_endpointDescriptonKeyAndValueMap.put("value", primeMap.get("DataService_endpointDescripton").toString());
        extraValueList.add(dataService_endpointDescriptonKeyAndValueMap);

        //dataservice_servesDataset 속성들
        LinkedHashMap<String, String> dataservice_servesDatasetKeyAndValueMap = new LinkedHashMap<String, String>();
        dataservice_servesDatasetKeyAndValueMap.put("key", "dataservice_servesDataset");
        dataservice_servesDatasetKeyAndValueMap.put("value", primeMap.get("DataService_servesDataset").toString());
        extraValueList.add(dataservice_servesDatasetKeyAndValueMap);

        //전체 CkanMeta가 될 맵에 넣는다.
        primeMap.put("extras", extraValueList);

        logger.info("Creation ckan_meta data");

        logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(primeMap));

        String ckanMeta = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(primeMap);

        FileOutputStream metaFos = new FileOutputStream(target);
        byte[] ckanMetaBytesByte = ckanMeta.getBytes();
        metaFos.write(ckanMetaBytesByte);
        metaFos.flush();
        metaFos.close();
    }
}
