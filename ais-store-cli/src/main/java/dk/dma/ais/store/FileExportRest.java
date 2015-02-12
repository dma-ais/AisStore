/* Copyright (c) 2011 Danish Maritime Authority.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.dma.ais.store;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPacketFilters;
import dk.dma.ais.packet.AisPacketOutputSinks;
import dk.dma.ais.reader.AisReader;
import dk.dma.ais.reader.AisReaders;
import dk.dma.commons.app.AbstractCommandLineTool;
import dk.dma.commons.util.io.OutputStreamSink;

/**
 * @author Jens Tuxen
 * @author David Andersen Camre
 */
public class FileExportRest extends AbstractCommandLineTool {

    private FileOutputStream fileOutputStream;
    private OutputStream outputStream;
    private OutputStreamSink<AisPacket> sink;
    private AtomicLong counter;

    // Status Variables
    long timeStart = System.currentTimeMillis();

    DecimalFormat decimalFormatter = new DecimalFormat("0.00");

    // Meta data
    private long currentTimeStamp;
    private Long packageCount = 0L;
    private long lastFlushTimestamp;

    private long intervalStartTime;

    Interval intervalVar;

    private long lastLoadedTimestamp;

    private String metaFileName;

    String printPrefix = "[AIS-STORE] ";

    /** A date time formatter for utc. */
    DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-M-d'T'HH:mm:ss'Z");

    @Parameter(names = "-mmsi", description = "Extract from mmsi schema")
    List<Integer> mmsis = new ArrayList<Integer>();

    @Parameter(names = "-filter", description = "The filter to apply")
    String filter;

    @Parameter(names = "-interval", description = "The ISO 8601 time interval for data to export")
    String interval;

    @Parameter(names = { "-area", "-box", "-geo" }, description = "Extract from geopgraphic cells schema within bounding box lat1,lon1,lat2,lon2")
    String area;

    @Parameter(names = "-outputFormat", description = "Output format, options: raw, json, jsonObject (use -columns), kml, kmz, table (use -columns)")
    String outputFormat = "raw";

    @Parameter(names = "-columns", description = "Optional columns, used for jsonObject and table.")
    String columns;

    @Parameter(names = "-separator", description = "Optional separator, used for table format.")
    String separator = ",";

    @Parameter(names = { "-file", "-output", "-o" }, description = "File to extract to (default is stdout)")
    String filePath;

    @Parameter(names = { "-fileName" }, description = "File to extract to (default is stdout)")
    String fileName;

    @Parameter(names = "-fetchSize", description = "internal fetch size buffer")
    Integer fetchSize = -1;

    @Parameter(names = "-force", description = "Disregard existing download and force redownload")
    boolean forceDownload = false;

    /** {@inheritDoc} */
    @Override
    protected void run(Injector injector) throws Exception {
        printAisStoreNL("AIS STORE COMMAND LINE TOOL INITIATED");
        printAisStoreLine();

        // Hardcoded values
        // interval = "2015-1-5T14:00:00Z/2015-1-5T14:10:00Z";
        // java -jar ais-store-cli-0.3-SNAPSHOT.jar export -area 15,-18,-10,14 -filter
        // "m.country=DNK & t.pos within bbox(15,-18,-10,14) & (t.lat<-0.3|t.lat>0.3) & (t.lon<-0.3|t.lon>0.3)" -fetchSize 30000
        // -interval
        // java -jar ais-store-cli-0.3-SNAPSHOT.jar export -area 15,-18,-10,14 -filter
        // "m.country=DNK & t.pos within bbox(15,-18,-10,14) & (t.lat<-0.3|t.lat>0.3) & (t.lon<-0.3|t.lon>0.3)" -fetchSize 30000
        // -interval 2015-1-5T14:00:00Z/2015-1-5T14:10:00Z

        // Create request
        String request = "";

        if (interval == null || interval.equals("")) {
            printAisStoreNL("No Interval provided, please check your request.");

            // return;
            terminateAndPrintHelp();
        }

        try {
            intervalVar = Interval.parse(interval);
        } catch (Exception e) {
            printAisStoreNL("Invalid Interval provided, please check your request.");
            terminateAndPrintHelp();
        }

        // intervalVar = DateTimeUtil.toInterval(intervalStr);
        intervalStartTime = intervalVar.getStartMillis();
        String intervalStr = interval.toString();

        request = request + "?interval=" + interval;

        // System.out.println("Interval parsed correct " + intervalStr);

        // Check if interval is valid, throw exception etc

        // Create task for exception throwing if args are required
        // If error, throw exception of error description then -help

        if (mmsis.size() > 0) {
            request = request + "&mmsi=";

            for (int i = 0; i < mmsis.size() - 1; i++) {
                request = request + mmsis.get(i) + ",";
            }

            request = request + mmsis.get(mmsis.size() - 1);
        }

        // filter
        // Google URL Encoder
        // "t.name like H* & t.name like HAMLET"
        // CHeck if url filter is valid, then url encode it and add to request

        // filter = "t.name like H* & t.name like HAMLET";
        // filter = "s.country in (DNK)";

        if (filter != null && !filter.equals("")) {
            String encodedFilter = URLEncoder.encode(filter, "UTF-8");

            try {
                AisPacketFilters.parseExpressionFilter(filter);
            } catch (Exception e) {
                printAisStoreNL("Invalid filter expression");
                terminateAndPrintHelp();
            }

            request = request + "&filter=" + encodedFilter;
        }

        // area
        // "&box=lat1,lon1,lat2,lon2"
        // blabla check if valid

        if (area != null && !area.equals("")) {
            request = request + "&box=" + area;
        }

        // If table, make sure column is added
        if ((columns == null || columns.equals("")) && (outputFormat.equals("table") || outputFormat.equals("jsonObject"))) {
            printAisStoreNL("When using outputFormat " + outputFormat + ", columns are required");
            terminateAndPrintHelp();
        }
        
        try {
            sink = AisPacketOutputSinks.getOutputSink(outputFormat,columns,separator);

            request = request + "&outputFormat=" + outputFormat;
        } catch (Exception e) {
            printAisStoreNL("Invalid output format provided, " + outputFormat + ", please check your request.");
            terminateAndPrintHelp();
        }        

        // if table do shit
        // columns \/ REQUIRED
        // "columns=mmsi;time;timestamp"

        // Check if valid
        // Split on ";"
        // "columns=<listElement0>:list1"

        if (columns != null) {
            request = request + "&columns=" + columns;
        }

        // seperator
        // "seperator=\t"
        // Url encode and add
        if (separator != null || !separator.equals("")) {
            String encodedSeparator = URLEncoder.encode(separator, "UTF-8");
            request = request + "&seperator=" + encodedSeparator;
        }

        // fetchSize
        if (fetchSize != -1) {
            request = request + "&fetchsize=" + fetchSize;
        }

        // Get path from request, if none it will store in root of ais store client
        // filePath = "C:\\AisStoreData\\";

        if (filePath == null) {
            filePath = "";
        } else {
            filePath = filePath + "/";
        }

        // No filename provided, generate unique based on request parameters
        if (fileName == null || fileName.equals("")) {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            String hex = (new HexBinaryAdapter()).marshal(md5.digest(request.getBytes()));
            fileName = hex;
        }

        // Generate unique hashsum based on request
        metaFileName = fileName + ".aisstore";

        // boolean isTryResume = true;

        // If we are trying to resume, don't override previous file

        try {
            fileOutputStream = new FileOutputStream(filePath + fileName, !forceDownload);
        } catch (Exception e) {
            printAisStoreNL("Error occuring writing to disk, make sure the folder path exists ");
            terminateAndPrintHelp();
        }

        outputStream = new BufferedOutputStream(fileOutputStream);

        // Should we resume anything
        // We have read the file
        // If the file exists that means a previous transaction has been done

        // Do we resume, if we do, we need to find the resume point ie move the start interval

        /**
         * System.out.println("Test Compare"); System.out.println(intervalStr);
         * 
         * DateTime time = new DateTime(interval.getStartMillis(), DateTimeZone.UTC); DateTime time2 = new
         * DateTime(interval.getEndMillis(), DateTimeZone.UTC);
         * 
         * String newIntervalStr = dateTimeFormatter.withZoneUTC().print(time) + "/" + dateTimeFormatter.withZoneUTC().print(time2);
         * System.out.println(newIntervalStr); // DateTime dateTime =
         * dateTimeFormatter.parseDateTime("15-Oct-2013 11:34:26 AM").withZone(DateTimeZone.UTC);
         * 
         * // System.out.println(dateTime);
         * 
         * // Interval var = Interval.parse(intervalStr); // String dateStr = formatter.withZone(DateTimeZone.UTC).print(dateTime1);
         * //
         * 
         * System.exit(0);
         **/

        printAisStoreNL("Request generation complete.");
        printAisStoreNL("AIS Data will be saved to " + filePath + fileName);
        // System.out.println("--------------------------------------------------------------------------------");

        // We are resuming, insert a Carriage Return Line Feed
        if (!forceDownload) {

            // Load the meta data in
            readMeta();

            // We have processed some packages already
            if (packageCount != 0) {

                String str = "\r\n";
                outputStream.write(str.getBytes());
                printAisStoreLine();
                printAisStoreNL("Resume detected - Updating Request");
                // System.out.println("From " + intervalStr);

                // Update intervalStr
                DateTime time = new DateTime(lastLoadedTimestamp, DateTimeZone.UTC);
                DateTime time2 = new DateTime(intervalVar.getEndMillis(), DateTimeZone.UTC);

                intervalStr = dateTimeFormatter.withZoneUTC().print(time) + "/" + dateTimeFormatter.withZoneUTC().print(time2);
                // System.out.println("To " + intervalStr);
                printAisStoreLine();

                // System.out.println("The last stored timestamp was \n" + lastLoadedTimestamp);
                // Interval interval2 = DateTimeUtil.toInterval(intervalStr);
                // System.out.println(interval2.getStartMillis());

            } else {
                writeMetaInit(intervalVar.getStartMillis());
                lastLoadedTimestamp = intervalVar.getStartMillis();
            }
        } else {
            // We are starting a new request, create a new meta init
            writeMetaInit(intervalVar.getStartMillis());
            lastLoadedTimestamp = intervalVar.getStartMillis();

        }
        // System.out.println("Interval Str is " + intervalStr);
        // System.exit(0);

        // Initialize
        counter = new AtomicLong(packageCount);

        // Do we need to set a new interval start based on the meta data read?

        DefaultHttpClient httpClient = new DefaultHttpClient();

        HttpHost target = new HttpHost("ais2.e-navigation.net", 443, "https");

        HttpGet getRequest = new HttpGet("/aisview/rest/store/query?interval=" + intervalStr);
        // HttpGet getRequest = new
        // HttpGet("/aisview/rest/store/query?interval=2015-1-1T10:00:00Z/2015-2-1T10:10:00Z&box=65.145,-5.373,34.450,76.893");
        // + "&mmsi=219230000"
        // + "&mmsi=219230000"
        printAisStoreNL("Executing request to " + target);

        HttpResponse httpResponse = httpClient.execute(target, getRequest);
        HttpEntity entity = httpResponse.getEntity();

        // Check we have an OK from server etc.

        printAisStoreLine();

        boolean terminateFailure = false;

        StatusLine reply = httpResponse.getStatusLine();
        switch (reply.getStatusCode()) {
        case HttpStatus.SC_OK:
            printAisStoreNL("Server Accepted Connection, download will begin shortly");
            printAisStoreLine();
            break;
        default:
            printAisStoreNL("An error occured establishing connection to the server. ");
            printAisStoreNL("Server returned Status Code " + reply.getStatusCode() + " with " + reply.getReasonPhrase());
            terminateFailure = true;
            break;
        }

        if (terminateFailure) {
            return;
        }

        // System.out.println("Got reply " + reply.getReasonPhrase() + " status code " + reply.getStatusCode());

        // String httpServerReply = httpResponse.getStatusLine();
        // System.out.println(httpResponse.getStatusLine());
        //
        // Header[] headers = httpResponse.getAllHeaders();
        // for (int i = 0; i < headers.length; i++) {
        // System.out.println(headers[i]);
        // }

        // Do we use the footer?

        AisReader aisReader;

        if (entity != null) {
            InputStream inputStream = entity.getContent();

            aisReader = aisReadWriter(inputStream);

            aisReader.start();
            aisReader.join();

            // Write the remainder still stored in buffer, update the final meta data with the finished data
            writeMetaUpdate(currentTimeStamp, counter.get());

            // Write the footer
            sink.footer(outputStream, counter.get());

            // Closer and flush the buffer
            outputStream.flush();
            outputStream.close();

            // Close and flush the file stream
            fileOutputStream.flush();
            fileOutputStream.close();
        }

        // print a new line to move on from previous /r

        printAisStoreNL("Downloading AIS Data 100% Estimated Time Left: 00:00:00                                       ");
        printAisStoreLine();
        printAisStoreNL("DOWNLOAD SUCCESS");
        printAisStoreLine();

        // We know current time
        long currentTime = System.currentTimeMillis();
        // How long have we been running
        long millis = currentTime - timeStart;
        String timeLeftStr = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millis),
                TimeUnit.MILLISECONDS.toMinutes(millis) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)),
                TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)));

        printAisStoreNL("Total Time " + timeLeftStr);
        printAisStoreNL("Finished at: " + new Date());
        printAisStoreNL("Messages recieved " + counter);

        // printAisStore("Query took " + timeLeftStr);
    }

    private void printDownloadStatus() {
        // Determine how far left we have to go

        // We know current time
        long currentTime = System.currentTimeMillis();
        // How long have we been running
        long milisecondsRunning = currentTime - timeStart;

        // Calculate total % done:

        // Total AIS time to process in miliseconds
        long aisTimeTotalOriginal = intervalVar.getEndMillis() - intervalStartTime;

        // Stretch of processed AIS time:
        long processedAisTimeOriginal = lastFlushTimestamp - intervalStartTime;

        double percentDoneOriginal = (double) processedAisTimeOriginal / (double) aisTimeTotalOriginal * 100;

        // Calculate the estimated time

        // Total AIS time to process in miliseconds
        long aisTimeTotal = intervalVar.getEndMillis() - lastLoadedTimestamp;

        // Stretch of processed AIS time:
        long processedAisTime = lastFlushTimestamp - lastLoadedTimestamp;
        double percentDoneNow = (double) processedAisTime / (double) aisTimeTotal * 100;

        double goToPercent = (100 - percentDoneNow);

        // How many % do we calculate pr. milisecond
        double percentPrMilisecond = percentDoneNow / ((double) milisecondsRunning);
        double timeLeft = goToPercent / percentPrMilisecond;

        long millis = (long) timeLeft;

        String timeLeftStr = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millis),
                TimeUnit.MILLISECONDS.toMinutes(millis) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)),
                TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)));

        String percentDoneStr = decimalFormatter.format(percentDoneOriginal) + "%";
        // String percentDoneStr = ((double) ((int) (percentDoneOriginal * 100))) / 100 + "%";

        // String part1DownloadMessage = "Downloading AIS Data " + percentDoneStr;
        // String part2DownloadMessage = " Estimated Time Left: " + timeLeftStr;

        // if (part1DownloadMessage.length() < )

        printAisStore("Downloading AIS Data " + percentDoneStr + " Estimated Time Left: " + timeLeftStr + "\r");
        // printAisStore();
        // int seconds = (int) (timeLeft / 1000) % 60;
        // int minutes = (int) ((timeLeft / (1000 * 60)) % 60);
        // int hours = (int) ((timeLeft / (1000 * 60 * 60)) % 24);
        // System.out.println(hours + ":" + minutes + ":" + seconds);

        // System.out.println("Miliseconds running" + milisecondsRunning);

        //
        // // AIS Time left of request
        // long aisTimeLeft = intervalVar.getEndMillis() - lastFlushTimestamp;
        //
        // // How long have we spent parsing up to now
        // long aisTimeParsed = processedAisTime / milisecondsRunning;
        //
        // System.out.println("We have in " + milisecondsRunning + " miliseconds processed " + processedAisTime + " AIS Interval");
        // System.out.println("AIS Time Left: " + aisTimeLeft);
        // System.out.println("aisTimeParsed " + aisTimeParsed);

        // counterCurrent.get();

        //

        // If we are resuming our % will be further along

        // We know our count since start
        // We know how long we have been running

    }

    /**
     * Read the meta file containing data regarding previous transactions in
     * 
     * The meta file will have the following fields: file name - the file containing the data, if no filename is provided in request
     * we use hash of request to ensure uniqueness last time stamp - the last recorded timestamp packageCount - the amount of
     * messages written, used in footer
     * 
     * @param path
     * @param metaFileName
     */
    private void readMeta() throws IOException, ParseException {

        File f = new File(filePath + metaFileName);
        if (f.exists() && !f.isDirectory()) {

            JSONParser parser = new JSONParser();

            try {

                Object obj = parser.parse(new FileReader(filePath + metaFileName));

                JSONObject jsonObject = (JSONObject) obj;

                // if (jsonObject.get("filename") != null) {
                fileName = (String) jsonObject.get("filename");
                // System.out.println(fileName);

                // }

                lastLoadedTimestamp = (Long) jsonObject.get("timestamp");
                // System.out.println(lastFlushTimestamp);

                packageCount = (Long) jsonObject.get("packageCount");

                // System.out.println(packageCount);

            }

            catch (Exception e) {
                e.printStackTrace();
                System.out.println("No meta file detected or invalid meta file");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void writeMetaInit(Long startInterval) {
        JSONObject obj = new JSONObject();
        obj.put("filename", fileName);
        obj.put("timestamp", startInterval);
        obj.put("packageCount", packageCount);

        try {
            // System.out.println("Writing");
            FileWriter file = new FileWriter(filePath + metaFileName);
            file.write(obj.toJSONString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @SuppressWarnings("unchecked")
    private void writeMetaUpdate(long timestamp, long packageCount) {

        // System.out.println("Updating meta!");
        FileWriter file = null;
        JSONObject obj = new JSONObject();
        obj.put("filename", fileName);
        obj.put("timestamp", timestamp);
        obj.put("packageCount", new Long(packageCount));

        try {

            file = new FileWriter(filePath + metaFileName);
            file.write(obj.toJSONString());
            file.flush();

        } catch (IOException e) {
            // e.printStackTrace();
            System.out.println("Failed to write because reasons " + e.getMessage());
        } finally {
            try {
                file.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.out.println("Failed to close file! " + e.getMessage());
            }
        }
    }

    private AisReader aisReadWriter(InputStream in) throws Exception {

        AisReader r = AisReaders.createReaderFromInputStream(in);
        r.registerPacketHandler(new Consumer<AisPacket>() {

            @Override
            public void accept(AisPacket t) {
                AisMessage message = t.tryGetAisMessage();

                // System.out.println(message.toString());

                currentTimeStamp = t.getBestTimestamp();

                if (lastLoadedTimestamp >= currentTimeStamp) {
                    // System.out.println("Skipping Message!");
                    return;
                }

                if (message == null) {
                    return;
                }

                try {
                    sink.process(outputStream, t, counter.incrementAndGet());

                    if (currentTimeStamp != lastFlushTimestamp && counter.get() % 10000 == 0) {

                        // We have a new timestamp sequence
                        lastFlushTimestamp = currentTimeStamp;

                        writeMetaUpdate(lastFlushTimestamp, counter.get());

                        // Force flush on both

                        outputStream.flush();
                        fileOutputStream.flush();

                        // Write
                        // lastFlushTimestamp
                        // timestamp

                        // if (counter.get() >= 1000) {
                        // System.out.println("Terminating as part of test! Last written timestamp was " + lastFlushTimestamp);
                        // System.exit(0);
                        // }

                        // Update user on progress
                        printDownloadStatus();
                    }

                    //

                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        });

        return r;

    }

    public static void main(String[] args) throws Exception {
        new FileExportRest().execute(args);
    }

    private void terminateAndPrintHelp() {
        System.out.println("Terminating AIS Store Command Line");

        try {
            new FileExportRest().execute(new String[] { "-help" });
        } catch (Exception e) {
        }
        System.exit(-1);
    }

    private void printAisStore(String toPrint) {
        System.out.print(printPrefix + toPrint);
    }

    private void printAisStoreNL(String toPrint) {
        System.out.println(printPrefix + toPrint);
    }

    private void printAisStoreLine() {
        System.out.println(printPrefix + "--------------------------------------------------------------------------------");
    }
}
