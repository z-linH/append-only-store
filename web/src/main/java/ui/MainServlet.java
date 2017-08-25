package ui;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Tuple;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import indexingTopology.common.data.DataTuple;
import model.DataBean;
import model.PageBean;
import util.StringUtil;

@WebServlet("/main")
public class MainServlet extends HttpServlet{

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private DataBean dataBean;

    private int xLow = 0;

    private int xHigh = 0;

    private int yLow = 0;

    private int yHigh = 0;

    private int time = 0;

    SearchTest searchTest;

    int rows;
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        this.doPost(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        request.setCharacterEncoding("utf-8");
        /**
         * test hzl
         */


//        String page=request.getParameter("page");
        DataBean oldDataBean = (DataBean) request.getSession().getAttribute("oldDataBean");
        xLow = Integer.parseInt(request.getParameter("xLow"));
        xHigh = Integer.parseInt(request.getParameter("xHigh"));
        yLow = Integer.parseInt(request.getParameter("yLow"));
        yHigh = Integer.parseInt(request.getParameter("yHigh"));
        time = Integer.parseInt(request.getParameter("time"));
        System.out.println(xLow+ " " + yLow + " " + xHigh + " " + yHigh + " " + time);
//        if(StringUtil.isEmpty(page)) {
//            page = "1";
//        }
//        if(null == oldDataBean){
        searchTest = new SearchTest(xLow, xHigh, yLow, yHigh, time);
        dataBean = searchTest.executeQuery();
//        }else{
//            dataBean = oldDataBean;
//        }
//        System.out.println(oldDataBean);
//        System.out.println(dataBean);
//        rows = dataBean.getTuples().size();
//        PageBean pageBean=new PageBean(Integer.parseInt(page),4);
            List<DataTuple> diaryList = dataBean.getTuples();
            List<String> fieldNames = dataBean.getFieldNames();
            long queryTime = dataBean.getTime();
            String time = String.valueOf(queryTime);
            String size = String.valueOf(fieldNames.size());
/*            for(int i=0;i<rows;i++){
                diaryList.add(""+i);
            }*/
//            int total=rows;
//            String pageCode=this.genPagation(total, Integer.parseInt(page), 4);
            request.getSession().setAttribute("dataBean", dataBean);
//            request.setAttribute("pageCode", pageCode);
            request.setAttribute("listSize",size);
            request.setAttribute("diaryList", diaryList);
            request.setAttribute("fieldNames", fieldNames);
            request.setAttribute("queryTime",time);
//            int nowPage = Integer.parseInt(request.getParameter("page"));
//            int beginPage = (nowPage-1)*4;
//            int endPage = nowPage*4;
//            request.setAttribute("beginPage",beginPage);
//            request.setAttribute("endPage",endPage);
            request.getRequestDispatcher("tables_dynamic.jsp").forward(request,response);

    }

/*
    private String genPagation(int totalNum,int currentPage,int pageSize){
        int totalPage=totalNum%pageSize==0?totalNum/pageSize:totalNum/pageSize+1;
        StringBuffer pageCode=new StringBuffer();
        pageCode.append("<li><a href='main?page=1'>首页</a></li>");
        if(currentPage==1){
            pageCode.append("<li class='disabled'><a href='#'>上一页</a></li>");
        }else{
            pageCode.append("<li><a href='main?page="+(currentPage-1)+"'>上一页</a></li>");
        }
        for(int i=currentPage-2;i<=currentPage+2;i++){
            if(i<1||i>totalPage){
                continue;
            }
            if(i==currentPage){
                pageCode.append("<li class='active'><a href='#'>"+i+"</a></li>");
            }else{
                pageCode.append("<li><a href='main?page="+i+"'>"+i+"</a></li>");
            }
        }
        if(currentPage==totalPage){
            pageCode.append("<li class='disabled'><a href='#'>下一页</a></li>");
        }else{
            pageCode.append("<li><a href='main?page="+(currentPage+1)+"'>下一页</a></li>");
        }
        pageCode.append("<li><a href='main?page="+totalPage+"'>尾页</a></li>");
        return pageCode.toString();
    }
*/



}
