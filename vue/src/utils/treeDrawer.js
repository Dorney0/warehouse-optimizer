export function drawTree(root, svgElement) {
    if (!svgElement) return;
    while (svgElement.firstChild) {
        svgElement.removeChild(svgElement.firstChild);
    }

    const nodeRadius = 50;
    const levelHeight = 100;
    const horizontalSpacing = 150;

    const nodesPositions = [];

    // Расчёт позиций узлов с layout
    function layout(node, depth, offsetX) {
        let width = 0;
        if (!node.children || node.children.length === 0) {
            nodesPositions.push({ node, x: offsetX, y: depth * levelHeight });
            return nodeRadius * 2;
        }

        let childX = offsetX;
        for (const child of node.children) {
            const w = layout(child, depth + 1, childX);
            childX += w + horizontalSpacing;
            width += w + horizontalSpacing;
        }
        width -= horizontalSpacing; // убрать последний пробел

        // Позиция родителя — середина между детьми (сдвинута на -nodeRadius)
        const parentX = offsetX + width / 2 - nodeRadius;
        nodesPositions.push({ node, x: parentX, y: depth * levelHeight });

        return width;
    }

    layout(root, 0, nodeRadius);

    // Отрисовка линий (стрелок) между узлами
    for (const pos of nodesPositions) {
        if (!pos.node.children) continue;
        const posByNode = new Map(nodesPositions.map(p => [p.node, p]))

        for (const child of pos.node.children) {
            const childPos = posByNode.get(child)

            if (!childPos) continue;

            const x1 = pos.x + nodeRadius;
            const y1 = pos.y + nodeRadius;
            const x2 = childPos.x + nodeRadius;
            const y2 = childPos.y + nodeRadius;

            const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
            line.setAttribute("x1", x1);
            line.setAttribute("y1", y1);
            line.setAttribute("x2", x2);
            line.setAttribute("y2", y2);
            line.setAttribute("stroke", "#666");
            line.setAttribute("stroke-width", 2);
            svgElement.appendChild(line);

            const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
            const midX = (x1 + x2) / 2;
            const midY = (y1 + y2) / 2;

            text.setAttribute("x", midX);
            text.setAttribute("y", midY - 5);
            text.setAttribute("text-anchor", "middle");
            text.setAttribute("fill", "#333");
            text.style.fontSize = "15px";
            text.style.userSelect = "none";

            text.textContent = child.quantity ?? "";

            svgElement.appendChild(text);
        }
    }

    // Отрисовка узлов (кругов) и подписей
    for (const pos of nodesPositions) {
        const g = document.createElementNS("http://www.w3.org/2000/svg", "g");

        const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
        circle.setAttribute("cx", pos.x + nodeRadius);
        circle.setAttribute("cy", pos.y + nodeRadius);
        circle.setAttribute("r", nodeRadius);
        circle.setAttribute("fill", "#007acc");
        circle.setAttribute("stroke", "#004a99");
        circle.setAttribute("stroke-width", 2);
        g.appendChild(circle);

        const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
        text.setAttribute("x", pos.x + nodeRadius);
        text.setAttribute("y", pos.y + nodeRadius - 10);
        text.setAttribute("text-anchor", "middle");
        text.setAttribute("fill", "#fff");
        text.style.fontSize = "15px";
        text.style.userSelect = "none";

        const words = pos.node.name.split(" ");
        const lineHeight = 14;

        words.forEach((word, i) => {
            const tspan = document.createElementNS("http://www.w3.org/2000/svg", "tspan");
            tspan.setAttribute("x", pos.x + nodeRadius);
            tspan.setAttribute("dy", i === 0 ? "0" : lineHeight);
            tspan.textContent = word;
            text.appendChild(tspan);
        });

        g.appendChild(text);

        svgElement.appendChild(g);
    }

    // Автоматическое масштабирование SVG под содержимое + динамическая ширина от контейнера
    const bbox = svgElement.getBBox();
    svgElement.setAttribute('viewBox', `${bbox.x} ${bbox.y} ${bbox.width} ${bbox.height}`);

    // Получить ширину контейнера
    const container = svgElement.closest('.tree-container');
    if (container) {
        const containerWidth = container.getBoundingClientRect().width;
        svgElement.setAttribute('width', containerWidth);
    } else {
        svgElement.setAttribute('width', bbox.width);
    }

    svgElement.setAttribute('height', bbox.height);

}