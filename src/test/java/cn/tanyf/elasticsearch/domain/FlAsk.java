package cn.tanyf.elasticsearch.domain;

import java.io.Serializable;

public class FlAsk implements Serializable {
	private static final long serialVersionUID = 1L;

	private Integer id;

    private String title;

    private String username;

    private String asktime;

    private Integer sex;

    private String tel;

    private String email;

    private Integer areacode;

    private Integer profid;

    private String address;

    private Integer isFinished;

    private Integer mysqlid;

    private Integer sid1;

    private Integer sid2;

    private Integer sid3;

    private String mobil;

    private String qq;

    private String classid;

    private String userid;

    private Integer isSendsms;

    private Integer isClass;

    private Integer isChange;

    private Integer classTime;

    private String classOper;

    private Integer returnvistStatu;

    private Integer twoclass;

    private String content;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	

	public String getAsktime() {
		return asktime;
	}

	public void setAsktime(String asktime) {
		this.asktime = asktime;
	}

	public Integer getSex() {
		return sex;
	}

	public void setSex(Integer sex) {
		this.sex = sex;
	}

	public String getTel() {
		return tel;
	}

	public void setTel(String tel) {
		this.tel = tel;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public Integer getAreacode() {
		return areacode;
	}

	public void setAreacode(Integer areacode) {
		this.areacode = areacode;
	}

	public Integer getProfid() {
		return profid;
	}

	public void setProfid(Integer profid) {
		this.profid = profid;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public Integer getIsFinished() {
		return isFinished;
	}

	public void setIsFinished(Integer isFinished) {
		this.isFinished = isFinished;
	}

	
	public Integer getMysqlid() {
		return mysqlid;
	}

	public void setMysqlid(Integer mysqlid) {
		this.mysqlid = mysqlid;
	}

	public Integer getSid1() {
		return sid1;
	}

	public void setSid1(Integer sid1) {
		this.sid1 = sid1;
	}

	public Integer getSid2() {
		return sid2;
	}

	public void setSid2(Integer sid2) {
		this.sid2 = sid2;
	}

	public Integer getSid3() {
		return sid3;
	}

	public void setSid3(Integer sid3) {
		this.sid3 = sid3;
	}

	public String getMobil() {
		return mobil;
	}

	public void setMobil(String mobil) {
		this.mobil = mobil;
	}

	public String getQq() {
		return qq;
	}

	public void setQq(String qq) {
		this.qq = qq;
	}

	public String getClassid() {
		return classid;
	}

	public void setClassid(String classid) {
		this.classid = classid;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public Integer getIsSendsms() {
		return isSendsms;
	}

	public void setIsSendsms(Integer isSendsms) {
		this.isSendsms = isSendsms;
	}

	public Integer getIsClass() {
		return isClass;
	}

	public void setIsClass(Integer isClass) {
		this.isClass = isClass;
	}

	public Integer getIsChange() {
		return isChange;
	}

	public void setIsChange(Integer isChange) {
		this.isChange = isChange;
	}

	public Integer getClassTime() {
		return classTime;
	}

	public void setClassTime(Integer classTime) {
		this.classTime = classTime;
	}

	public String getClassOper() {
		return classOper;
	}

	public void setClassOper(String classOper) {
		this.classOper = classOper;
	}

	public Integer getReturnvistStatu() {
		return returnvistStatu;
	}

	public void setReturnvistStatu(Integer returnvistStatu) {
		this.returnvistStatu = returnvistStatu;
	}

	public Integer getTwoclass() {
		return twoclass;
	}

	public void setTwoclass(Integer twoclass) {
		this.twoclass = twoclass;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

    @Override
    public String toString() {
        return "FlAsk [id=" + id + ", title=" + title + ", content=" + content + "]";
    }
}