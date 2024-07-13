import com.ym.gmall.common.base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BaseAppTest extends BaseApp {

    public static void main(String[] args) {
        new BaseAppTest().start(9999, 1, "dim:gmall:test", "topic_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }
}
