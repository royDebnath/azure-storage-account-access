package com.azure.storage.controller;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.storage.StorageCredentialsToken;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RestController
@RequestMapping("storage-access-api")
public class BlobController {

    public static final String ACTIVE_DIRECTORY_ENDPOINT_URL = "https://login.microsoftonline.com/";
    public static final String AZURE_STORAGE_BASE_URL = "https://storage.azure.com/";
    public static final String HTTPS = "https://";
    public static final String BLOB_URL_SUFFIX = "blob.core.windows.net/";
    public static final String DOT = ".";
    public static final String LINE_BREAK = "<br><br>";

    @Value("${azure.storage.account.name}")
    private String storageAccountName;

    @Value("${azure.storage.container.name}")
    private String storageContainerName;

    @Value("${azure.tenant.id}")
    private String tenantId;

    @Value("${azure.ad.client.id}")
    private String clientId;

    @Value("${azure.ad.client.secret}")
    private String clientSecret;

    private CloudBlobContainer getCloudBlobContainer() throws URISyntaxException, StorageException {
        StorageCredentialsToken credentialsToken = new StorageCredentialsToken(storageAccountName, getToken());
        CloudBlobClient blobClient = new CloudBlobClient(new URI(HTTPS + storageAccountName + DOT + BLOB_URL_SUFFIX), credentialsToken);
        return blobClient.getContainerReference(storageContainerName);
    }

    private String getToken() {
        String ACCESS_TOKEN = null;
        ExecutorService service = Executors.newFixedThreadPool(1);
        AuthenticationContext context = null;
        try {
            context = new AuthenticationContext(ACTIVE_DIRECTORY_ENDPOINT_URL + tenantId, false, service);
            ClientCredential credential = new ClientCredential(clientId, clientSecret);
            Future<AuthenticationResult> future = context.acquireToken(AZURE_STORAGE_BASE_URL, credential, null);
            ACCESS_TOKEN = future.get().getAccessToken();
        } catch (InterruptedException | ExecutionException | MalformedURLException e) {
            e.printStackTrace();
        } finally {
            service.shutdown();
        }
        return ACCESS_TOKEN;
    }

    @GetMapping("/list-blobs")
    public ResponseEntity getAllBlobs() throws URISyntaxException, StorageException {
        List uris = new ArrayList<>();
        for (ListBlobItem blobItem : getCloudBlobContainer().listBlobs()) {
            uris.add(blobItem.getUri());
        }
        return ResponseEntity.ok(uris);
    }

    @PostMapping("/upload-blob")
    public ResponseEntity upload(@RequestParam MultipartFile multipartFile) throws URISyntaxException, StorageException, IOException {
        CloudBlockBlob blob = getCloudBlobContainer().getBlockBlobReference(multipartFile.getOriginalFilename());
        blob.upload(multipartFile.getInputStream(), -1);
        URI uri = blob.getUri();
        return ResponseEntity.ok(uri);
    }

    @DeleteMapping("/delete-blob")
    public ResponseEntity delete(@RequestParam String blobName) throws URISyntaxException, StorageException {
        CloudBlockBlob blobToBeDeleted = getCloudBlobContainer().getBlockBlobReference(blobName);
        blobToBeDeleted.deleteIfExists();
        return ResponseEntity.ok().build();
    }
}











