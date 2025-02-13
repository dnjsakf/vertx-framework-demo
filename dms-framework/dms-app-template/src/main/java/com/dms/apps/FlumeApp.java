package com.dms.apps;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.flume.node.Application;
import org.apache.flume.node.PropertiesFileConfigurationProvider;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlumeApp extends Application {
    
    public static void main(String[] args) {

        FlumeApp flumeApp = new FlumeApp();

        // File properties = flumeApp.getConfigFileStream("properties/application.properties");
        File configFile = flumeApp.getConfigFileStream("properties/flume.properties");

        // Flume Initialize
		PropertiesFileConfigurationProvider configurationProvider = new PropertiesFileConfigurationProvider(
            "dms_agent",
            configFile
        );

		flumeApp.handleConfigurationEvent(configurationProvider.getConfiguration());
		flumeApp.start();

        
        // Runtime 종료 후 producer와 executorService 종료
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            flumeApp.stop();
            log.info("Stoped");
        }));
        
    }

    public File getConfigFileStream(String resoucePath) {
        // 리소스 파일을 InputStream으로 얻기
        InputStream inputStream = FlumeApp.class.getClassLoader().getResourceAsStream(resoucePath);
        File resourceFile = null;
    
        if (inputStream != null) {
            try {
                // 임시 파일 생성
                Path tempFile = Files.createTempFile("flume", ".conf");
                Files.copy(inputStream, tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                resourceFile = tempFile.toFile();

                // 절대 경로 추출
                String absolutePath = tempFile.toAbsolutePath().toString();
                System.out.println("Absolute Path: " + absolutePath);

            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Resource file not found!");
        }

        return resourceFile;
    }

    public File getAbsolutePath(String resourcePath) {
        
        // 리소스 파일 URL 얻기
        URL resourceUrl = FlumeApp.class.getClassLoader().getResource(resourcePath);
        File resourceFile = null;

        if (resourceUrl != null) {
            // 리소스 파일을 File 객체로 변환
            resourceFile = new File(resourceUrl.getFile());
            // 절대 경로 추출
            System.out.println("Absolute Path: " + resourceFile.getAbsolutePath());
        } else {
            // 파일 없음
            System.out.println("Resource file not found!");
        }

        return resourceFile;
    }
}
