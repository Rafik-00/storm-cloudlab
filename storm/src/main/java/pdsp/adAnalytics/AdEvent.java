package pdsp.adAnalytics;

import java.io.Serializable;

public class AdEvent implements Serializable {
    public String type;
    public int clicks;
    public int views;
    public String display_url;
    public String adId;
    public String advertiserId;
    public int depth;
    public int position;
    public String queryId;
    public String keywordId;
    public String titleId;
    public String descriptionId;
    public String userId;
    public float count;

    public AdEvent() {
    }

    public AdEvent(String type, int clicks, int views, String display_url, String adId, String queryId, int depth, int position, String advertiserId, String keywordId, String titleId, String descriptionId, String userId, float count) {
        this.type = type;
        this.clicks = clicks;
        this.views = views;
        this.display_url = display_url;
        this.adId = adId;
        this.advertiserId = advertiserId;
        this.depth = depth;
        this.position = position;
        this.queryId = queryId;
        this.keywordId = keywordId;
        this.titleId = titleId;
        this.descriptionId = descriptionId;
        this.userId = userId;
        this.count = count;
    }

    // Getters and Setters

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getClicks() {
        return clicks;
    }

    public void setClicks(int clicks) {
        this.clicks = clicks;
    }

    public int getViews() {
        return views;
    }

    public void setViews(int views) {
        this.views = views;
    }

    public String getDisplay_url() {
        return display_url;
    }

    public void setDisplay_url(String display_url) {
        this.display_url = display_url;
    }

    public String getAdId() {
        return adId;
    }

    public void setAdId(String adId) {
        this.adId = adId;
    }

    public String getAdvertiserId() {
        return advertiserId;
    }

    public void setAdvertiserId(String advertiserId) {
        this.advertiserId = advertiserId;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getKeywordId() {
        return keywordId;
    }

    public void setKeywordId(String keywordId) {
        this.keywordId = keywordId;
    }

    public String getTitleId() {
        return titleId;
    }

    public void setTitleId(String titleId) {
        this.titleId = titleId;
    }

    public String getDescriptionId() {
        return descriptionId;
    }

    public void setDescriptionId(String descriptionId) {
        this.descriptionId = descriptionId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public float getCount() {
        return count;
    }

    public void setCount(float count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "AdEvent{" +
                "type='" + type + '\'' +
                ", clicks=" + clicks +
                ", views=" + views +
                ", display_url='" + display_url + '\'' +
                ", adId=" + adId +
                ", advertiserId=" + advertiserId +
                ", depth=" + depth +
                ", position=" + position +
                ", queryId=" + queryId +
                ", keywordId=" + keywordId +
                ", titleId=" + titleId +
                ", descriptionId=" + descriptionId +
                ", userId=" + userId +
                ", count=" + count +
                '}';
    }
}
