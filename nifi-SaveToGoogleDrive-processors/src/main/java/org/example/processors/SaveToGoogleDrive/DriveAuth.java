package org.example.processors.SaveToGoogleDrive;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;

import java.io.*;
import java.util.*;

/* class to demonstarte use of Drive files list API */
public class DriveAuth {
    private static final String APPLICATION_NAME = "Nifi";

    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    public static Drive driveServices(String clientSecret) throws Exception {
        final HttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        GoogleCredential credential = GoogleCredential
                .fromStream(new FileInputStream(clientSecret))
                .createScoped(Collections.singletonList(DriveScopes.DRIVE));
        Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME)
                .build();
        return service;
    }

}