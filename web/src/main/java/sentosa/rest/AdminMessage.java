package sentosa.rest;

import com.alibaba.fastjson.JSONObject;
import indexingTopology.util.track.TrackNew;
import indexingTopology.util.track.TrackSearchWs;
import indexingTopology.util.track.TrackSpacialSearch;
import indexingTopology.util.track.TrackSpacialSearchWs;
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
        System.out.println(str);
//        TrackSpacialSearchWs trackSpacialSearch = new TrackSpacialSearchWs();
//        String result = trackSpacialSearch.services(null,str);
        TrackNew trackNew = new TrackNew();
        String result = trackNew.service(null,str);
//        TrackSearchWs trackSearchWs = new TrackSearchWs();
//        String result = trackSearchWs.services(null,str);
        if (message.equals("null")) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("message", NaiveQueryImpl.instance().getAdminMessage());
            return jsonObject.toString();
        } else {
            NaiveQueryImpl.instance().setAdminMessage(message);
//            JSONObject jsonObject = new JSONObject();
//            jsonObject.put("response", result);
            System.out.println(result);
            return result;
        }
    }
}
