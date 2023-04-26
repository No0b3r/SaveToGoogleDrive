/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example.processors.SaveToGoogleDrive;

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveRequest;
import com.google.api.services.drive.model.File;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static java.util.Collections.singletonList;

@Tags({"example"})
@CapabilityDescription("Saves contents to Google Drive")
@ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the Google Drive object")
@WritesAttributes({
        @WritesAttribute(attribute = "id", description = "Id of the file"),
        @WritesAttribute(attribute = "filename", description = "The name of the file"),
        @WritesAttribute(attribute = "mimetype", description = "The MIME type of the file"),
        @WritesAttribute(attribute = "size", description = "The size of the file"),
        @WritesAttribute(attribute = "errorMessage", description = "The error message")})
public class SaveToGoogleDrive extends AbstractProcessor {

    //==---------Properties & Relationships-------------------------------------------------
    private static final PropertyDescriptor FOLDER_ID = new PropertyDescriptor.Builder()
            .name("FOLDER_ID")
            .displayName("Folder Id")
            .description("Id of drive's folder")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
            .name("FILE_NAME")
            .displayName("File name")
            .description("Name of the file to upload")
            .defaultValue("${filename}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final PropertyDescriptor CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("CLIENT_SECRET")
            .displayName("Client Secret File")
            .description("Path to your client secret that generated from GCP")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully transferred is routed to this relationship".trim())
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that fails to be transferred is routed to this relationship".trim())
            .build();
    
    //==---------------------------------------------------------------

    private List<PropertyDescriptor> properties;

    private Set<Relationship> relationships;

    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = new ArrayList<>();
        properties.add(FOLDER_ID);
        properties.add(FILE_NAME);
        properties.add(CLIENT_SECRET);
        properties = Collections.unmodifiableList(properties);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
    }
    
    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        String folderId = context.getProperty(FOLDER_ID).evaluateAttributeExpressions(flowFile).getValue();
        String filename = context.getProperty(FILE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        String clientSecret = context.getProperty(CLIENT_SECRET).evaluateAttributeExpressions(flowFile).getValue();
        String mimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
        long size = flowFile.getSize();

        Drive driveService = null;
        try {
            driveService = DriveAuth.driveServices(clientSecret);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            File fileMetadata = createMetadata(filename, folderId);
            File uploadedFile;

            try (InputStream inputStream = session.read(flowFile);
                 BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream)) {

                InputStreamContent mediaContent = new InputStreamContent(mimeType, bufferedInputStream);
                mediaContent.setLength(size);

                DriveRequest<File> driveRequest = driveRequest(driveService, fileMetadata, mediaContent);

                uploadedFile = driveRequest.execute();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            flowFile = session.putAllAttributes(flowFile, createAttributes(uploadedFile));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            e.printStackTrace();
            flowFile = session.putAttribute(flowFile, "errorMessage", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    //==---------------------------------------------------------------
    private File createMetadata(String name, String parentId) {
        File metadata = new File();
        metadata.setName(name);
        metadata.setParents(singletonList(parentId));
        return metadata;
    }
    private DriveRequest<File> driveRequest(Drive driveService, File fileMetadata, InputStreamContent mediaContent) throws IOException {
        if (fileMetadata.getId() == null) {
            return driveService.files()
                    .create(fileMetadata, mediaContent)
                    .setSupportsAllDrives(true)
                    .setFields("id, name, mimeType, size");
        } else {
            return driveService.files()
                    .update(fileMetadata.getId(), new File(), mediaContent)
                    .setSupportsAllDrives(true)
                    .setFields("id, name, mimeType, size");
        }
    }

    public Map<String, String> createAttributes(File file) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("id", file.getId());
        attributes.put("name", file.getName());
        attributes.put("mimeType", file.getMimeType());
        attributes.put("size", String.valueOf(file.getSize()));
        return attributes;
    }

}
