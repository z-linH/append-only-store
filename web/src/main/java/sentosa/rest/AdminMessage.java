package sentosa.rest;


import com.alibaba.fastjson.JSONObject;
import indexingTopology.util.track.PosNonSpacialSearchWs;
import indexingTopology.util.track.PosSpacialSearchWs;
import sentosa.query.naive.NaiveQueryImpl;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

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
    @Produces(MediaType.TEXT_PLAIN)
    public String getWarningMessage(@DefaultValue("null") @QueryParam("message") String message) {
        String str = message.replace('[','{');
        str = str.replace(']','}');
        PosSpacialSearchWs posSpacialSearchWs = new PosSpacialSearchWs();
        String result = posSpacialSearchWs.service(null, str);
        System.out.println(result);
        System.out.println(str);
        if (message.equals("null")) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("message", NaiveQueryImpl.instance().getAdminMessage());
            return jsonObject.toString();
        } else {
            NaiveQueryImpl.instance().setAdminMessage(message);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("response", result);
            return result;
        }
    }
}
