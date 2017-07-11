package cn.tanyf.elasticsearch.domain;

import java.io.Serializable;

/**
 * TODO: 增加描述
 * 
 * @author tan.yf
 * @date 2017年5月26日 下午4:47:17
 */
public class Goods implements Serializable {
	private static final long serialVersionUID = 1L;
	private Long id;
	private String title;
	private String content;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
}
