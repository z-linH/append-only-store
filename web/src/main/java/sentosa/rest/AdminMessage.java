package sentosa.rest;



import com.alibaba.fastjson.JSONObject;
import indexingTopology.util.track.PosNonSpacialSearchWs;
import indexingTopology.util.track.PosSpacialSearchWs;
import org.apache.commons.codec.binary.StringUtils;
import sentosa.compress.GZIPUtils;
import sentosa.query.naive.NaiveQueryImpl;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Created by robert on 28/12/16.
 */
@Path("admin_message")
public class AdminMessage {
    @POST
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.TEXT_PLAIN)
    public String setWainingMessage(
            @FormParam("message") String message) {
        NaiveQueryImpl.instance().setAdminMessage(message);
        return "Success!";
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject getWarningMessage(@DefaultValue("null") @QueryParam("message") String message) throws IOException {
        String str = message.replace('[','{');
        str = str.replace(']','}');

        System.out.println(str);
//        TrackSpacialSearchWs trackSpacialSearch = new TrackSpacialSearchWs();
//        String result = trackSpacialSearch.services(null,str);

//        TrackNew trackNew = new TrackNew();
//        String result = trackNew.service(null,str);

//        TrackSearchWs trackSearchWs = new TrackSearchWs();
//        String result = trackSearchWs.services(null,str);

        PosSpacialSearchWs posSpacialSearchWs = new PosSpacialSearchWs();
        String result = posSpacialSearchWs.service(null, str);
        System.out.println(result);
//        System.out.println(str);
//        if (message.equals("null")) {
//            JSONObject jsonObject = new JSONObject();
//            jsonObject.put("message", NaiveQueryImpl.instance().getAdminMessage());
//            return jsonObject.toString();
//        } else {
        NaiveQueryImpl.instance().setAdminMessage(message);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("response", GZIPUtils.compress(result));
//        System.out.println(result.length());
//        JSONObject jsonObject = JSONObject.parseObject(result);
        System.out.println("原长度：" + result.length());
        System.out.println("压缩后长度" + GZIPUtils.compress(result).length());
        System.out.println("------------------------------------------------------------");
        String s = GZIPUtils.compress(result);
        System.out.println(GZIPUtils.uncompress(s));
        return jsonObject;
//        }
    }
}
