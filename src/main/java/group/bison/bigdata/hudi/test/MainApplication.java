package group.bison.bigdata.hudi.test;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

public class MainApplication {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("hudi-datalake")
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.hive.convertMetastoreParquet", "false")
                .getOrCreate();

        sparkSession.sparkContext().hadoopConfiguration().set("fs.defaultFS", "s3a://addx-test");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", System.getenv("s3AccessKey"));
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", System.getenv("s3SecretKey"));
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-north-1.amazonaws.com.cn");

        Album album = new Album();
        album.setAlbumId(2L);
        album.setTitle("hello");
        album.setTracks(new String[]{"1", "2", "3"});
        album.setUpdateDate(System.currentTimeMillis());

        Dataset dataset = sparkSession.createDataset(Collections.singletonList(album), ExpressionEncoder.javaBean(Album.class));
        upsert(dataset, "album", "albumId", "updateDate");

        sparkSession.read().format("hudi").load("/tmp/store/album/*").show();

        sparkSession.read().format("hudi").option("as.of.instant", "2022-07-28 14:11:08.200").load("/tmp/store/album/*").show();
    }

    public static void upsert(Dataset dataset, String tableName, String key, String combineKey) {
        dataset.write()
                .format("hudi")
                .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), key)
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), combineKey)
                .option(HoodieWriteConfig.TABLE_NAME, tableName)
                .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL())
                // Ignore this property for now, the default is too high when experimenting on your local machine
                // Set this to a lower value to improve performance.
                // I'll probably cover Hudi tuning in a separate post.
                .option("hoodie.upsert.shuffle.parallelism", "2")
                .mode(SaveMode.Append)
                .save("/tmp/store/album/");
    }

    public static Long dateToLong(String dateString) {
        return LocalDate.parse(dateString, DateTimeFormatter.ISO_ORDINAL_DATE).toEpochDay();
    }

    public static class Album {
        Long albumId;

        String title;

        String[] tracks;

        Long updateDate;

        public Long getAlbumId() {
            return albumId;
        }

        public void setAlbumId(Long albumId) {
            this.albumId = albumId;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String[] getTracks() {
            return tracks;
        }

        public void setTracks(String[] tracks) {
            this.tracks = tracks;
        }

        public Long getUpdateDate() {
            return updateDate;
        }

        public void setUpdateDate(Long updateDate) {
            this.updateDate = updateDate;
        }
    }

}
