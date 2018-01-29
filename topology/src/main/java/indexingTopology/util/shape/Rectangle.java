package indexingTopology.util.shape;

import java.io.Serializable;

/**
 * Create by zelin on 17-12-5
 **/
public class Rectangle implements Shape, Serializable{

    private double leftTopX, leftTopY;
    private double rightBottomX, rightBottomY;

    public Rectangle(Point leftTop, Point rightBottom) {
        this.leftTopX = leftTop.x;
        this.leftTopY = leftTop.y;
        this.rightBottomX = rightBottom.x;
        this.rightBottomY = rightBottom.y;
    }

    public double getLeftTopX() {
        return leftTopX;
    }

    public double getLeftTopY() {
        return leftTopY;
    }

    public double getRightBottomX() {
        return rightBottomX;
    }

    public double getRightBottomY() {
        return rightBottomY;
    }

    @Override
    public boolean checkIn(Point point) {
        double pointX = point.x, pointY = point.y;
        if((pointX <= rightBottomX && pointX >= leftTopX) && (pointY <= leftTopY && pointY >= rightBottomY)) {
            return true;
        }
        return false;
    }

    @Override
    public Rectangle getExternalRectangle() {
        return this;
    }
}
