import org.junit.Assert;
import org.junit.Test;

public class KeyGenTest {

    @Test
    public void testVBucket() throws Exception {
        Assert.assertEquals(KeyGen.vBucket("zzz_cb_dummy_255"), 1);
        Assert.assertEquals(KeyGen.vBucket("zzz_cb_dummy_5"), 120);
        Assert.assertEquals(KeyGen.vBucket("zzz_cb_dummy_9488"), 125);
    }

}